package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testResinPlatformID = "11111111-1111-1111-1111-111111111111"

func TestResinConfigGetMasksSecretsAndUsesEnvironmentFallback(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "proxy-secret")
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Platform = "Default"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = "http://resin.test"
	})
	response := actionManagementForTest(t, plugin, "resin.config.get", nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.StatusCode, response.Body)
	}
	text := string(response.Body)
	for _, forbidden := range []string{"admin-secret", "proxy-secret"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("config response leaked %q: %s", forbidden, text)
		}
	}
	var decoded struct {
		OK   bool            `json:"ok"`
		Data resinConfigView `json:"data"`
	}
	if errUnmarshal := json.Unmarshal(response.Body, &decoded); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if decoded.Data.Configured || !decoded.Data.AdminTokenSet || !decoded.Data.ProxyTokenSet {
		t.Fatalf("unexpected config view: %#v", decoded.Data)
	}
}

func TestResinConfigSaveLocalOnlyWithoutAPI(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	params := map[string]any{
		"enabled":                           true,
		"local_only":                        true,
		"platform":                          "Default",
		"platform_id":                       testResinPlatformID,
		"admin_url":                         "http://resin.local",
		"admin_token":                       "admin-secret",
		"proxy_token":                       "proxy-secret",
		"proxy_template":                    "socks5h://{platform}.{account}:{proxy_token}@resin:2260",
		"auto_rotate_after_quality_failure": true,
	}
	response := actionManagementForTest(t, plugin, "resin.config.save", params)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.StatusCode, response.Body)
	}
	state := plugin.store.Snapshot()
	if !state.Resin.Configured || !state.Resin.Enabled || !state.Resin.LocalOnly {
		t.Fatalf("local resin config was not persisted: %#v", state.Resin)
	}
	if state.Resin.PlatformID != "" || state.Resin.AdminURL != "" || state.Resin.AdminToken != "" || state.Resin.ProxyToken != "" {
		t.Fatalf("local resin config persisted API credentials: %#v", state.Resin)
	}
	if state.Resin.ValidatedAt.IsZero() {
		t.Fatal("local resin config did not record activation time")
	}
	settings := plugin.ResinConfig()
	if !settings.LocalOnly || settings.PlatformID != "" || settings.AdminURL != "" || settings.AdminToken != "" || settings.ProxyToken != "" {
		t.Fatalf("local resin settings exposed API fields: %#v", settings)
	}
	var decoded struct {
		OK   bool            `json:"ok"`
		Data resinConfigView `json:"data"`
	}
	if errUnmarshal := json.Unmarshal(response.Body, &decoded); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if !decoded.Data.LocalOnly || !decoded.Data.Configured || !decoded.Data.Enabled {
		t.Fatalf("unexpected local config view: %#v", decoded.Data)
	}
	validateResponse := actionManagementForTest(t, plugin, "resin.config.validate", map[string]any{"local_only": true})
	if validateResponse.StatusCode != http.StatusOK || !strings.Contains(string(validateResponse.Body), `"local_only":true`) {
		t.Fatalf("local validation failed: status=%d body=%s", validateResponse.StatusCode, validateResponse.Body)
	}
}

func TestResinConfigValidateDoesNotPersist(t *testing.T) {
	adminServer, socksAddr := newResinValidationServers(t, "admin-secret", "proxy-secret", true)
	defer adminServer.Close()
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	params := validResinParams(adminServer.URL, socksAddr)
	response := actionManagementForTest(t, plugin, "resin.config.validate", params)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.StatusCode, response.Body)
	}
	text := string(response.Body)
	for _, forbidden := range []string{"admin-secret", "proxy-secret", socksAddr} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("validation response leaked %q: %s", forbidden, text)
		}
	}
	state := plugin.store.Snapshot()
	if state.Resin.Configured {
		t.Fatalf("validate persisted resin config: %#v", state.Resin)
	}
}

func TestResinConfigSavePersistsValidatedSettingsAndPreservesBlankTokens(t *testing.T) {
	adminServer, socksAddr := newResinValidationServers(t, "admin-secret", "proxy-secret", true)
	defer adminServer.Close()
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	params := validResinParams(adminServer.URL, socksAddr)
	params["platform"] = "default"
	response := actionManagementForTest(t, plugin, "resin.config.save", params)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.StatusCode, response.Body)
	}
	state := plugin.store.Snapshot()
	if state.Resin.Platform != "Default" {
		t.Fatalf("saved platform was not canonicalized from platform name: %#v", state.Resin)
	}
	if !state.Resin.Configured || state.Resin.AdminToken != "admin-secret" || state.Resin.ProxyToken != "proxy-secret" {
		t.Fatalf("validated resin config was not persisted: %#v", state.Resin)
	}
	if state.Resin.ValidatedAt.IsZero() {
		t.Fatal("validated_at was not persisted")
	}
	blankTokens := validResinParams(adminServer.URL, socksAddr)
	blankTokens["admin_token"] = ""
	blankTokens["proxy_token"] = ""
	response = actionManagementForTest(t, plugin, "resin.config.save", blankTokens)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("second save status = %d body=%s", response.StatusCode, response.Body)
	}
	state = plugin.store.Snapshot()
	if state.Resin.AdminToken != "admin-secret" || state.Resin.ProxyToken != "proxy-secret" {
		t.Fatalf("blank token fields cleared stored secrets: %#v", state.Resin)
	}
	if strings.Contains(string(response.Body), "admin-secret") || strings.Contains(string(response.Body), "proxy-secret") {
		t.Fatalf("save response leaked secret: %s", response.Body)
	}
}

func TestResinConfigSaveRejectsInvalidAdminTokenWithoutPersisting(t *testing.T) {
	adminServer, socksAddr := newResinValidationServers(t, "admin-secret", "proxy-secret", true)
	defer adminServer.Close()
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	params := validResinParams(adminServer.URL, socksAddr)
	params["admin_token"] = "wrong-secret"
	response := actionManagementForTest(t, plugin, "resin.config.save", params)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", response.StatusCode, response.Body)
	}
	if plugin.store.Snapshot().Resin.Configured {
		t.Fatal("invalid admin token was persisted")
	}
	if strings.Contains(string(response.Body), "wrong-secret") || strings.Contains(string(response.Body), "admin-secret") {
		t.Fatalf("error response leaked token: %s", response.Body)
	}
}

func TestResinConfigSaveRejectsInvalidProxyTokenWithoutPersisting(t *testing.T) {
	adminServer, socksAddr := newResinValidationServers(t, "admin-secret", "proxy-secret", false)
	defer adminServer.Close()
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	params := validResinParams(adminServer.URL, socksAddr)
	params["proxy_token"] = "wrong-proxy-secret"
	response := actionManagementForTest(t, plugin, "resin.config.save", params)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", response.StatusCode, response.Body)
	}
	if plugin.store.Snapshot().Resin.Configured {
		t.Fatal("invalid proxy token was persisted")
	}
	if strings.Contains(string(response.Body), "wrong-proxy-secret") || strings.Contains(string(response.Body), "proxy-secret") {
		t.Fatalf("error response leaked token: %s", response.Body)
	}
}

func validResinParams(adminURL, socksAddr string) map[string]any {
	return map[string]any{
		"enabled":                           true,
		"platform":                          "Default",
		"platform_id":                       testResinPlatformID,
		"admin_url":                         adminURL,
		"admin_token":                       "admin-secret",
		"proxy_token":                       "proxy-secret",
		"proxy_template":                    "socks5h://{platform}.{account}:{proxy_token}@" + socksAddr,
		"auto_rotate_after_quality_failure": true,
	}
}

func newResinValidationServers(t *testing.T, adminToken, proxyToken string, acceptProxy bool) (*httptest.Server, string) {
	t.Helper()
	adminServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+adminToken {
			writer.WriteHeader(http.StatusUnauthorized)
			_, _ = writer.Write([]byte(`{"error":{"code":"UNAUTHORIZED"}}`))
			return
		}
		switch request.URL.Path {
		case "/api/v1/system/info":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"version":"test"}`))
		case "/api/v1/platforms":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"items":[{"id":"` + testResinPlatformID + `","name":"Default"}],"total":1,"limit":1000,"offset":0}`))
		case "/api/v1/platforms/" + testResinPlatformID:
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"id":"` + testResinPlatformID + `","name":"Default"}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	socksAddr := startSocks5AuthServer(t, "Default.grok-validation", proxyToken, acceptProxy)
	return adminServer, socksAddr
}

func startSocks5AuthServer(t *testing.T, expectedUsername, expectedPassword string, accept bool) string {
	t.Helper()
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatal(errListen)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, errAccept := listener.Accept()
			if errAccept != nil {
				return
			}
			go handleTestSocks5Auth(conn, expectedUsername, expectedPassword, accept)
		}
	}()
	return listener.Addr().String()
}

func handleTestSocks5Auth(conn net.Conn, expectedUsername, expectedPassword string, accept bool) {
	defer conn.Close()
	request := make([]byte, 3)
	if _, errRead := io.ReadFull(conn, request); errRead != nil {
		return
	}
	_, _ = conn.Write([]byte{0x05, 0x02})
	authHeader := make([]byte, 2)
	if _, errRead := io.ReadFull(conn, authHeader); errRead != nil {
		return
	}
	username := make([]byte, int(authHeader[1]))
	if _, errRead := io.ReadFull(conn, username); errRead != nil {
		return
	}
	passwordLength := make([]byte, 1)
	if _, errRead := io.ReadFull(conn, passwordLength); errRead != nil {
		return
	}
	password := make([]byte, int(passwordLength[0]))
	if _, errRead := io.ReadFull(conn, password); errRead != nil {
		return
	}
	status := byte(0x01)
	if accept && string(username) == expectedUsername && string(password) == expectedPassword {
		status = 0x00
	}
	_, _ = conn.Write([]byte{0x01, status})
}

func TestSocks5AuthenticateRejectsWrongPassword(t *testing.T) {
	socksAddr := startSocks5AuthServer(t, "user", "right-password", false)
	conn, errDial := net.Dial("tcp", socksAddr)
	if errDial != nil {
		t.Fatal(errDial)
	}
	defer conn.Close()
	errAuth := socks5Authenticate(conn, "user", "wrong-password")
	if errAuth == nil || !strings.Contains(errAuth.Error(), "rejected") {
		t.Fatalf("unexpected auth error: %v", errAuth)
	}
	if strings.Contains(fmt.Sprint(errAuth), "wrong-password") {
		t.Fatal("auth error leaked password")
	}
}

func TestResinConfigSaveRejectsMalformedAdminURL(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	params := validResinParams("ftp://resin.test", "127.0.0.1:1")
	response := actionManagementForTest(t, plugin, "resin.config.save", params)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", response.StatusCode, response.Body)
	}
	if plugin.store.Snapshot().Resin.Configured {
		t.Fatal("malformed config was persisted")
	}
}

func TestResinConfigSaveDoesNotPersistWhenValidationContextCancelled(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	params := validResinParams("http://127.0.0.1:1", "127.0.0.1:1")
	taskValue, status, errSave := plugin.actionResinConfigSave(ctx, params)
	if errSave != nil || status != http.StatusAccepted {
		t.Fatalf("save submission error = %v status=%d", errSave, status)
	}
	task, okTask := taskValue.(Task)
	if !okTask {
		t.Fatalf("save did not return a task: %#v", taskValue)
	}
	finished := waitTaskForTest(t, plugin, task.ID)
	if finished.Status != taskStatusFailed || finished.ErrorStatus != http.StatusBadGateway {
		t.Fatalf("save task result = %#v", finished)
	}
	if plugin.store.Snapshot().Resin.Configured {
		t.Fatal("cancelled validation persisted config")
	}
}

func TestValidateResinPlatformReturnsCanonicalName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"id":"` + testResinPlatformID + `","name":"Canonical"}`))
	}))
	defer server.Close()
	settings := ResinSettings{
		Platform:   "Stale",
		PlatformID: testResinPlatformID,
		AdminURL:   server.URL,
		AdminToken: "admin-secret",
	}
	name, errValidate := validateResinPlatform(context.Background(), settings)
	if errValidate != nil || name != "Canonical" {
		t.Fatalf("name=%q error=%v", name, errValidate)
	}
}

func TestResolveResinPlatformByName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/platforms" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		if request.URL.Query().Get("keyword") != "canonical" {
			t.Fatalf("keyword = %q", request.URL.Query().Get("keyword"))
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"items":[{"id":"` + testResinPlatformID + `","name":"Canonical"}],"total":1,"limit":1000,"offset":0}`))
	}))
	defer server.Close()
	settings := ResinSettings{
		Platform:   "canonical",
		AdminURL:   server.URL,
		AdminToken: "admin-secret",
	}
	resolved, errResolve := resolveResinPlatformByName(context.Background(), settings)
	if errResolve != nil {
		t.Fatal(errResolve)
	}
	if resolved.Platform != "Canonical" || resolved.PlatformID != testResinPlatformID {
		t.Fatalf("unexpected resolved platform: %#v", resolved)
	}
}
