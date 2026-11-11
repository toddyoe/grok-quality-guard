package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
)

func TestBindResinPreservesTokensAndMasksResult(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "proxy-secret")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer admin-secret" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		if request.URL.Path != "/api/v1/platforms/"+testResinPlatformID {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"id":"` + testResinPlatformID + `","name":"Canonical"}`))
	}))
	defer server.Close()
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Stale"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
		cfg.Resin.ProxyTemplate = "socks5h://{platform}.{account}:{proxy_token}@resin:2260"
	})
	result, errBind := plugin.BindResin(context.Background(), "auth-1", false)
	if errBind != nil {
		t.Fatal(errBind)
	}
	if !regexp.MustCompile(`^grok-alice-[0-9a-z]{4}$`).MatchString(result.Account) {
		t.Fatalf("unexpected resin account %q", result.Account)
	}
	if strings.Contains(result.ProxyURL, "proxy-secret") || strings.Contains(result.ProxyURL, "alice") {
		t.Fatalf("bind result leaked proxy credentials: %q", result.ProxyURL)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if authRawString(saved, "access_token") != "access-auth-1" || authRawString(saved, "refresh_token") != "refresh-auth-1" {
		t.Fatal("token fields were not preserved")
	}
	proxy := authRawString(saved, "proxy_url")
	if !strings.Contains(proxy, "Canonical."+result.Account) || !strings.Contains(proxy, "proxy-secret") {
		t.Fatalf("saved proxy URL did not use canonical platform name: %q", proxy)
	}
	if strings.Contains(proxy, "Stale.") {
		t.Fatalf("saved proxy URL used stale configured platform name: %q", proxy)
	}
	meta := guardMeta(saved)
	if !meta.Managed || meta.ResinAccount != result.Account || meta.ResinGeneration != 1 {
		t.Fatalf("unexpected metadata: %#v", meta)
	}
}

func TestBindResinRefusesForeignProxy(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{
		"proxy_url": "socks5h://user:pass@example.com:1080",
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.Resin.Enabled = true })
	result, errBind := plugin.BindResin(context.Background(), "auth-1", false)
	if errBind == nil || !result.Conflict {
		t.Fatalf("expected proxy conflict, got result=%#v err=%v", result, errBind)
	}
	if len(host.saves) != 0 {
		t.Fatal("foreign proxy was overwritten")
	}
}

func TestRotateResinDeletesLeaseAndWritesNewAccount(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "proxy-secret")
	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer admin-secret" {
			t.Errorf("authorization header not forwarded")
		}
		switch request.Method {
		case http.MethodGet:
			if request.URL.Path != "/api/v1/platforms/"+testResinPlatformID {
				t.Errorf("platform path = %s", request.URL.Path)
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"id":"` + testResinPlatformID + `","name":"Canonical"}`))
		case http.MethodDelete:
			deleted = append(deleted, request.URL.Path)
			writer.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("method = %s", request.Method)
		}
	}))
	defer server.Close()
	oldProxy := "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260"
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{
		"proxy_url": oldProxy,
		"grok_quality_guard": map[string]any{
			"managed":          true,
			"resin_account":    "grok-alice-old1",
			"resin_generation": 1,
		},
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Stale"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	if errRotate := plugin.RotateResin(context.Background(), "auth-1"); errRotate != nil {
		t.Fatal(errRotate)
	}
	if len(deleted) != 1 || !strings.Contains(deleted[0], "grok-alice-old1") {
		t.Fatalf("unexpected deleted paths: %#v", deleted)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	meta := guardMeta(saved)
	if meta.ResinAccount == "grok-alice-old1" || meta.ResinGeneration != 2 {
		t.Fatalf("resin account was not rotated: %#v", meta)
	}
	proxy := authRawString(saved, "proxy_url")
	if !strings.Contains(proxy, "Canonical."+meta.ResinAccount) {
		t.Fatalf("proxy_url did not use canonical platform name: %q", proxy)
	}
	if strings.Contains(proxy, "Stale.") {
		t.Fatalf("proxy_url used stale platform name: %q", proxy)
	}
	if authRawString(saved, "access_token") != "access-auth-1" || authRawString(saved, "refresh_token") != "refresh-auth-1" {
		t.Fatal("token fields were not preserved during rotation")
	}
	state := plugin.store.Snapshot()
	rotationIndex := -1
	for i := range state.Events {
		if state.Events[i].Action == "resin_rotate" {
			rotationIndex = i
			break
		}
	}
	if rotationIndex < 0 {
		t.Fatal("resin rotation event was not recorded")
	}
	wantDetail := "旧租约：grok-alice-old1 → 新租约：" + meta.ResinAccount
	if state.Events[rotationIndex].Detail != wantDetail {
		t.Fatalf("rotation event detail = %q, want %q", state.Events[rotationIndex].Detail, wantDetail)
	}
}

func TestClearResinProxyDeletesLeaseAndClearsAuth(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "")
	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer admin-secret" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch request.Method {
		case http.MethodGet:
			if request.URL.Path != "/api/v1/platforms/"+testResinPlatformID {
				t.Errorf("platform path = %s", request.URL.Path)
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"id":"` + testResinPlatformID + `","name":"Canonical"}`))
		case http.MethodDelete:
			deleted = append(deleted, request.URL.Path)
			writer.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("method = %s", request.Method)
		}
	}))
	defer server.Close()
	oldProxy := "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260"
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": oldProxy,
		"grok_quality_guard": map[string]any{
			"managed":          true,
			"resin_account":    "grok-alice-old1",
			"resin_generation": 2,
		},
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Default"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	result, errClear := plugin.ClearResinProxy(context.Background(), "auth-1")
	if errClear != nil {
		t.Fatal(errClear)
	}
	if !result.Cleared || !result.RemoteLeaseDeleted || result.Account != "grok-alice-old1" {
		t.Fatalf("unexpected clear result: %#v", result)
	}
	if strings.Contains(result.ProxyURL, "proxy-secret") || strings.Contains(result.ProxyURL, "alice") {
		t.Fatalf("clear result leaked proxy credentials: %q", result.ProxyURL)
	}
	if len(deleted) != 1 || !strings.Contains(deleted[0], "grok-alice-old1") {
		t.Fatalf("unexpected deleted paths: %#v", deleted)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if authRawString(saved, "proxy_url") != "" {
		t.Fatalf("proxy_url was not cleared: %q", authRawString(saved, "proxy_url"))
	}
	meta := guardMeta(saved)
	if meta.Managed || meta.ResinAccount != "" || meta.ResinGeneration != 2 {
		t.Fatalf("unexpected metadata after clear: %#v", meta)
	}
	if authRawString(saved, "access_token") != "access-auth-1" || authRawString(saved, "refresh_token") != "refresh-auth-1" {
		t.Fatal("token fields were not preserved")
	}
	stateAccount, okState := plugin.stateAccount("auth-1")
	if !okState || stateAccount.Resin.Managed || stateAccount.Resin.Account != "" || stateAccount.Resin.Generation != 2 {
		t.Fatalf("unexpected state after clear: %#v", stateAccount)
	}
	state := plugin.store.Snapshot()
	foundEvent := false
	for _, event := range state.Events {
		if event.Action == "resin_clear" && event.Result == "cleared" {
			foundEvent = true
		}
	}
	if !foundEvent {
		t.Fatal("resin clear event was not recorded")
	}
}

func TestClearExternalProxyDoesNotRequireResin(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": "socks5h://user:external-secret@example.com:1080",
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	result, errClear := plugin.ClearResinProxy(context.Background(), "auth-1")
	if errClear != nil {
		t.Fatal(errClear)
	}
	if !result.Cleared || result.RemoteLeaseDeleted || result.Account != "" {
		t.Fatalf("unexpected clear result: %#v", result)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if authRawString(saved, "proxy_url") != "" {
		t.Fatalf("external proxy_url was not cleared: %q", authRawString(saved, "proxy_url"))
	}
	if authRawString(saved, "access_token") != "access-auth-1" {
		t.Fatal("externally clear removed token fields")
	}
}

func TestClearManagedProxyLeaseFailureLeavesAuthUntouched(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "proxy-secret")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"id":"` + testResinPlatformID + `","name":"Default"}`))
		case http.MethodDelete:
			writer.WriteHeader(http.StatusInternalServerError)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260",
		"grok_quality_guard": map[string]any{
			"managed":       true,
			"resin_account": "grok-alice-old1",
		},
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Default"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	result, errClear := plugin.ClearResinProxy(context.Background(), "auth-1")
	if errClear == nil {
		t.Fatalf("expected lease delete failure, result=%#v", result)
	}
	if result.RemoteLeaseDeleted {
		t.Fatalf("lease was reported deleted after failure: %#v", result)
	}
	if len(host.saves) != 0 {
		t.Fatal("auth was written after failed lease deletion")
	}
}

func TestRotateResinLocalOnlyRewritesProxyWithoutAPI(t *testing.T) {
	oldProxy := "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260"
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": oldProxy,
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.LocalOnly = true
	})
	if errRotate := plugin.RotateResin(context.Background(), "auth-1"); errRotate != nil {
		t.Fatal(errRotate)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	meta := guardMeta(saved)
	if !meta.Managed || meta.ResinAccount == "" || meta.ResinAccount == "grok-alice-old1" || meta.ResinGeneration != 1 {
		t.Fatalf("unexpected local rotation metadata: %#v", meta)
	}
	rotatedProxy := authRawString(saved, "proxy_url")
	if !strings.Contains(rotatedProxy, "Default."+meta.ResinAccount) || !strings.Contains(rotatedProxy, "proxy-secret") {
		t.Fatalf("local rotation did not preserve platform and password: %q", rotatedProxy)
	}
	stateAccount, okState := plugin.stateAccount("auth-1")
	if !okState || !stateAccount.Resin.Managed || stateAccount.Resin.Account != meta.ResinAccount {
		t.Fatalf("unexpected local rotation state: %#v", stateAccount)
	}
	state := plugin.store.Snapshot()
	foundEvent := false
	for _, event := range state.Events {
		if event.Action == "resin_rotate" && strings.Contains(event.Detail, "未调用 Resin API") {
			foundEvent = true
		}
	}
	if !foundEvent {
		t.Fatal("local rotation event was not recorded")
	}
}

func TestClearResinProxyLocalOnlySkipsRemoteLease(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260",
		"grok_quality_guard": map[string]any{
			"managed":       true,
			"resin_account": "grok-alice-old1",
		},
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.LocalOnly = true
	})
	result, errClear := plugin.ClearResinProxy(context.Background(), "auth-1")
	if errClear != nil {
		t.Fatal(errClear)
	}
	if !result.Cleared || result.RemoteLeaseDeleted {
		t.Fatalf("unexpected local clear result: %#v", result)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if authRawString(saved, "proxy_url") != "" {
		t.Fatalf("proxy_url was not cleared: %q", authRawString(saved, "proxy_url"))
	}
	if authRawString(saved, "access_token") != "access-auth-1" {
		t.Fatal("local clear removed token fields")
	}
	foundEvent := false
	for _, event := range plugin.store.Snapshot().Events {
		if event.Action == "resin_clear" && strings.Contains(event.Detail, "remote_lease=skipped_local_only") {
			foundEvent = true
		}
	}
	if !foundEvent {
		t.Fatal("local clear event did not report skipped remote lease")
	}
}

func TestRotateAfterFailureLocalOnlyRewritesProxy(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260",
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.LocalOnly = true
		cfg.Resin.AutoRotateAfterQualityFailure = true
	})
	rotated, errRotate := plugin.rotateAfterFailure(context.Background(), "auth-1", ClassMissingThinking)
	if errRotate != nil {
		t.Fatal(errRotate)
	}
	if !rotated {
		t.Fatal("local auto-rotation did not run")
	}
	if len(host.saves) != 1 {
		t.Fatalf("expected one auth save, got %d", len(host.saves))
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	meta := guardMeta(saved)
	if !meta.Managed || meta.ResinAccount == "" || meta.ResinAccount == "grok-alice-old1" {
		t.Fatalf("unexpected auto-rotation metadata: %#v", meta)
	}
	if authRawString(saved, "access_token") != "access-auth-1" {
		t.Fatal("auto-rotation removed token fields")
	}
}

func TestBindResinRejectsLocalOnlyMode(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.LocalOnly = true
	})
	result, errBind := plugin.BindResin(context.Background(), "auth-1", false)
	if errBind == nil || result.Error == "" {
		t.Fatalf("expected local-only bind rejection, result=%#v err=%v", result, errBind)
	}
	if len(host.saves) != 0 {
		t.Fatal("local-only bind wrote auth")
	}
}

func TestBindUnboundResinRejectsLocalOnlyMode(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.LocalOnly = true
	})
	results, errBind := plugin.BindUnboundResin(context.Background(), false)
	if errBind == nil || results != nil {
		t.Fatalf("expected local-only bind-unbound rejection, results=%#v err=%v", results, errBind)
	}
}

func TestRewriteResinProxyAccountPreservesURL(t *testing.T) {
	rotated, errRotate := rewriteResinProxyAccount("socks5h://Default.grok-alice-old1:p%40ss@resin:2260/socks", "grok-alice-new1")
	if errRotate != nil {
		t.Fatal(errRotate)
	}
	parsed, errParse := url.Parse(rotated)
	if errParse != nil {
		t.Fatal(errParse)
	}
	if parsed.Scheme != "socks5h" || parsed.Host != "resin:2260" || parsed.Path != "/socks" {
		t.Fatalf("proxy endpoint changed: %q", rotated)
	}
	password, okPassword := parsed.User.Password()
	if parsed.User.Username() != "Default.grok-alice-new1" || !okPassword || password != "p@ss" {
		t.Fatalf("proxy credentials changed: %q", rotated)
	}
}

func TestRewriteResinProxyAccountRejectsUnsupportedUsername(t *testing.T) {
	cases := []string{
		"socks5h://user:pass@resin:2260",
		"socks5h://user@resin:2260",
		"socks5h://Default.account:pass@",
	}
	for _, proxyURL := range cases {
		if _, errRotate := rewriteResinProxyAccount(proxyURL, "grok-new1"); errRotate == nil {
			t.Fatalf("expected invalid proxy error for %q", proxyURL)
		}
	}
}

func TestDeleteResinLeaseStatusHandling(t *testing.T) {
	cases := []struct {
		status  int
		wantErr bool
	}{
		{status: http.StatusNoContent, wantErr: false},
		{status: http.StatusNotFound, wantErr: false},
		{status: http.StatusUnauthorized, wantErr: true},
		{status: http.StatusTooManyRequests, wantErr: true},
		{status: http.StatusInternalServerError, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(tc.status)
			}))
			defer server.Close()
			errDelete := deleteResinLease(context.Background(), server.URL, "platform-1", "token", "account")
			if (errDelete != nil) != tc.wantErr {
				t.Fatalf("delete error = %v, wantErr %v", errDelete, tc.wantErr)
			}
			if typed, ok := errDelete.(*resinDeleteError); ok && typed.Status != tc.status {
				t.Fatalf("error status = %d, want %d", typed.Status, tc.status)
			}
		})
	}
}

func TestRotateResinAdminAuthFailureLeavesAuthUntouched(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "proxy-secret")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{
		"proxy_url": "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260",
		"grok_quality_guard": map[string]any{
			"managed":       true,
			"resin_account": "grok-alice-old1",
		},
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Default"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	if errRotate := plugin.RotateResin(context.Background(), "auth-1"); errRotate == nil {
		t.Fatal("expected admin authentication failure")
	}
	if len(host.saves) != 0 {
		t.Fatal("auth was written after failed lease deletion")
	}
}

func TestRandomBase36Format(t *testing.T) {
	pattern := regexp.MustCompile(`^[0-9a-z]{4}$`)
	seen := make(map[string]struct{})
	for i := 0; i < 32; i++ {
		value, errValue := randomBase36(4)
		if errValue != nil {
			t.Fatal(errValue)
		}
		if !pattern.MatchString(value) {
			t.Fatalf("invalid base36 value %q", value)
		}
		seen[value] = struct{}{}
	}
	if len(seen) < 16 {
		t.Fatalf("random suffixes show excessive collisions: %d unique", len(seen))
	}
}

func TestMaskProxyURLDropsCredentialsAndPath(t *testing.T) {
	masked := maskProxyURL("socks5h://user:pass@example.com:1080/secret/path?token=abc")
	if masked != "socks5h://example.com:1080" {
		t.Fatalf("masked proxy = %q", masked)
	}
}

func TestResinBindResultJSONDoesNotContainToken(t *testing.T) {
	result := ResinBindResult{AuthID: "auth-1", Account: "grok-alice-abcd", ProxyURL: maskProxyURL("socks5h://u:secret@example.com:1080")}
	encoded, errMarshal := json.Marshal(result)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	if strings.Contains(string(encoded), "secret") {
		t.Fatalf("serialized result leaked secret: %s", encoded)
	}
}
