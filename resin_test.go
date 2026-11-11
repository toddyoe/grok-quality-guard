package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
