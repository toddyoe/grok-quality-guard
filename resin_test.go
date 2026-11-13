package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func seedResinState(t *testing.T, plugin *Plugin, account string, generation int64) {
	t.Helper()
	if errUpdate := plugin.stateStore().Update(func(state *State) error {
		accountState := state.Accounts["auth-1"]
		accountState.Resin = ResinState{Managed: true, Account: account, Generation: generation}
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}
}

func failResinStateWrites(t *testing.T, plugin *Plugin) {
	t.Helper()
	blocker := filepath.Join(t.TempDir(), "state-blocker")
	if errWrite := os.WriteFile(blocker, []byte("blocker"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	store := plugin.stateStore()
	store.mu.Lock()
	originalPath := store.path
	store.path = filepath.Join(blocker, "state.json")
	store.mu.Unlock()
	t.Cleanup(func() {
		store.mu.Lock()
		store.path = originalPath
		store.mu.Unlock()
	})
}

func newResinConcurrencyServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var active atomic.Int32
	var maxActive atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maxActive.Load()
			if current <= observed || maxActive.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		switch request.Method {
		case http.MethodGet:
			if request.URL.Path != "/api/v1/platforms/"+testResinPlatformID {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"id":"` + testResinPlatformID + `","name":"Default"}`))
		case http.MethodDelete:
			writer.WriteHeader(http.StatusNoContent)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	return server, &maxActive
}

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
	stateAccount, okState := plugin.stateAccount("auth-1")
	if !okState || !stateAccount.Resin.Managed || stateAccount.Resin.Account != result.Account || stateAccount.Resin.Generation != 1 {
		t.Fatalf("unexpected resin state: %#v", stateAccount)
	}
	assertNoGuardMetadata(t, saved)
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

func TestRotateResinQueuesLeaseDeleteAndWritesNewAccount(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "proxy-secret")
	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer admin-secret" {
			t.Errorf("authorization header not forwarded")
		}
		if request.Method != http.MethodDelete {
			t.Errorf("method = %s", request.Method)
			return
		}
		deleted = append(deleted, request.URL.Path)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	oldProxy := "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260"
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{
		"proxy_url": oldProxy,
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Canonical"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	seedResinState(t, plugin, "grok-alice-old1", 1)
	if errRotate := plugin.RotateResin(context.Background(), "auth-1"); errRotate != nil {
		t.Fatal(errRotate)
	}
	if len(deleted) != 0 {
		t.Fatalf("rotation called the Resin admin API synchronously: %#v", deleted)
	}
	if jobs := plugin.store.Snapshot().ResinLeaseDeletes; len(jobs) != 1 || jobs[0].Account != "grok-alice-old1" {
		t.Fatalf("old lease was not queued for deletion: %#v", jobs)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	stateAccount, okState := plugin.stateAccount("auth-1")
	if !okState || stateAccount.Resin.Account == "grok-alice-old1" || stateAccount.Resin.Generation != 2 {
		t.Fatalf("resin account was not rotated: %#v", stateAccount)
	}
	proxy := authRawString(saved, "proxy_url")
	if !strings.Contains(proxy, "Canonical."+stateAccount.Resin.Account) {
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
	wantDetail := "旧租约：grok-alice-old1 → 新租约：" + stateAccount.Resin.Account + "，旧租约删除已入队"
	if state.Events[rotationIndex].Detail != wantDetail {
		t.Fatalf("rotation event detail = %q, want %q", state.Events[rotationIndex].Detail, wantDetail)
	}
	if processed := plugin.processDueResinLeaseDeletes(context.Background(), time.Now().UTC()); processed != 1 {
		t.Fatalf("processed deletes = %d, want 1", processed)
	}
	if len(deleted) != 1 || !strings.Contains(deleted[0], "grok-alice-old1") {
		t.Fatalf("unexpected deleted paths: %#v", deleted)
	}
	if jobs := plugin.store.Snapshot().ResinLeaseDeletes; len(jobs) != 0 {
		t.Fatalf("successful delete was not removed from the queue: %#v", jobs)
	}
	assertNoGuardMetadata(t, saved)
}

func TestClearResinProxyQueuesLeaseDeleteAndClearsAuth(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "")
	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer admin-secret" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		if request.Method != http.MethodDelete {
			t.Errorf("method = %s", request.Method)
			return
		}
		deleted = append(deleted, request.URL.Path)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	oldProxy := "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260"
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": oldProxy,
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Default"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	seedResinState(t, plugin, "grok-alice-old1", 2)
	result, errClear := plugin.ClearResinProxy(context.Background(), "auth-1")
	if errClear != nil {
		t.Fatal(errClear)
	}
	if !result.Cleared || !result.RemoteLeaseDeleteQueued || result.Account != "grok-alice-old1" {
		t.Fatalf("unexpected clear result: %#v", result)
	}
	if strings.Contains(result.ProxyURL, "proxy-secret") || strings.Contains(result.ProxyURL, "alice") {
		t.Fatalf("clear result leaked proxy credentials: %q", result.ProxyURL)
	}
	if len(deleted) != 0 {
		t.Fatalf("clear called the Resin admin API synchronously: %#v", deleted)
	}
	if jobs := plugin.store.Snapshot().ResinLeaseDeletes; len(jobs) != 1 || jobs[0].Account != "grok-alice-old1" {
		t.Fatalf("old lease was not queued for deletion: %#v", jobs)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if authRawString(saved, "proxy_url") != "" {
		t.Fatalf("proxy_url was not cleared: %q", authRawString(saved, "proxy_url"))
	}
	assertNoGuardMetadata(t, saved)
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
		if event.Action == "resin_clear" && event.Result == "cleared" && strings.Contains(event.Detail, "remote_lease=delete_queued") {
			foundEvent = true
		}
	}
	if !foundEvent {
		t.Fatal("resin clear event was not recorded")
	}
	if processed := plugin.processDueResinLeaseDeletes(context.Background(), time.Now().UTC()); processed != 1 {
		t.Fatalf("processed deletes = %d, want 1", processed)
	}
	if len(deleted) != 1 || !strings.Contains(deleted[0], "grok-alice-old1") {
		t.Fatalf("unexpected deleted paths: %#v", deleted)
	}
}

func TestClearResinProxyInfersLeaseFromSavedConfig(t *testing.T) {
	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer admin-secret" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		if request.Method != http.MethodDelete {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		deleted = append(deleted, request.URL.Path)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	oldProxy := "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260"
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": oldProxy,
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	if errUpdate := plugin.stateStore().Update(func(state *State) error {
		state.Resin = ResinSettings{
			Configured:    true,
			Enabled:       true,
			Platform:      "Default",
			PlatformID:    testResinPlatformID,
			AdminURL:      server.URL,
			AdminToken:    "admin-secret",
			ProxyToken:    "proxy-secret",
			ProxyTemplate: "socks5h://{platform}.{account}:{proxy_token}@resin:2260",
			ValidatedAt:   time.Now().UTC(),
		}
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}

	result, errClear := plugin.ClearResinProxy(context.Background(), "auth-1")
	if errClear != nil {
		t.Fatal(errClear)
	}
	if !result.Cleared || !result.RemoteLeaseDeleteQueued || result.Account != "grok-alice-old1" {
		t.Fatalf("unexpected inferred clear result: %#v", result)
	}
	if len(deleted) != 0 {
		t.Fatalf("clear called the Resin admin API synchronously: %#v", deleted)
	}
	if processed := plugin.processDueResinLeaseDeletes(context.Background(), time.Now().UTC()); processed != 1 {
		t.Fatalf("processed deletes = %d, want 1", processed)
	}
	if len(deleted) != 1 || !strings.HasSuffix(deleted[0], "/grok-alice-old1") {
		t.Fatalf("managed lease was not deleted: %#v", deleted)
	}
}

func TestResinBindSelectedUsesBoundedQueue(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "proxy-secret")
	server, maxActive := newResinConcurrencyServer(t)

	var entries []pluginapi.HostAuthFileEntry
	auths := make(map[string]json.RawMessage)
	authIDs := []any{}
	for index := 1; index <= 4; index++ {
		authID := fmt.Sprintf("auth-%d", index)
		entry, raw := singleGrokAuth(t, authID, fmt.Sprintf("user%d@example.com", index), nil)
		entries = append(entries, entry...)
		for key, value := range raw {
			auths[key] = value
		}
		authIDs = append(authIDs, authID)
	}
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Probe.Concurrency = 2
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Default"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	response := actionManagementForTest(t, plugin, "resin.bind", map[string]any{"auth_ids": authIDs})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.StatusCode, response.Body)
	}
	if got := maxActive.Load(); got != 2 {
		t.Fatalf("max concurrent resin requests = %d, want 2", got)
	}
}

func TestResinBindUnboundUsesBoundedQueue(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "proxy-secret")
	server, maxActive := newResinConcurrencyServer(t)

	var entries []pluginapi.HostAuthFileEntry
	auths := make(map[string]json.RawMessage)
	for index := 1; index <= 4; index++ {
		authID := fmt.Sprintf("auth-%d", index)
		entry, raw := singleGrokAuth(t, authID, fmt.Sprintf("user%d@example.com", index), nil)
		entries = append(entries, entry...)
		for key, value := range raw {
			auths[key] = value
		}
	}
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Probe.Concurrency = 2
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Default"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	response := actionManagementForTest(t, plugin, "resin.bind_unbound", nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.StatusCode, response.Body)
	}
	if got := maxActive.Load(); got != 2 {
		t.Fatalf("max concurrent resin requests = %d, want 2", got)
	}
}

func TestResinClearSelectedQueuesLeaseDeletes(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "proxy-secret")
	server, maxActive := newResinConcurrencyServer(t)

	var entries []pluginapi.HostAuthFileEntry
	auths := make(map[string]json.RawMessage)
	authIDs := []any{}
	for index := 1; index <= 4; index++ {
		authID := fmt.Sprintf("auth-%d", index)
		account := fmt.Sprintf("grok-alice-%d", index)
		entry, raw := singleGrokAuth(t, authID, fmt.Sprintf("user%d@example.com", index), map[string]any{
			"proxy_url": "socks5h://Default." + account + ":proxy-secret@resin:2260",
		})
		entries = append(entries, entry...)
		for key, value := range raw {
			auths[key] = value
		}
		authIDs = append(authIDs, authID)
	}
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Probe.Concurrency = 2
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Default"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	if errUpdate := plugin.stateStore().Update(func(state *State) error {
		for index, authID := range authIDs {
			accountID := authID.(string)
			account := state.Accounts[accountID]
			if account == nil {
				account = &AccountState{AuthID: accountID, State: StateUnknown}
				state.Accounts[accountID] = account
			}
			account.Resin = ResinState{Managed: true, Account: fmt.Sprintf("grok-alice-%d", index+1), Generation: 1}
		}
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	response := actionManagementForTest(t, plugin, "resin.clear", map[string]any{"auth_ids": authIDs})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.StatusCode, response.Body)
	}
	if got := maxActive.Load(); got != 0 {
		t.Fatalf("clear called the Resin admin API synchronously: %d", got)
	}
	if jobs := plugin.store.Snapshot().ResinLeaseDeletes; len(jobs) != len(authIDs) {
		t.Fatalf("queued deletes = %d, want %d: %#v", len(jobs), len(authIDs), jobs)
	}
	if processed := plugin.processDueResinLeaseDeletes(context.Background(), time.Now().UTC()); processed != len(authIDs) {
		t.Fatalf("processed deletes = %d, want %d", processed, len(authIDs))
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("max concurrent lease deletes = %d, want 1", got)
	}
	if jobs := plugin.store.Snapshot().ResinLeaseDeletes; len(jobs) != 0 {
		t.Fatalf("successful deletes were not removed from the queue: %#v", jobs)
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
	if !result.Cleared || result.RemoteLeaseDeleteQueued || result.Account != "" {
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

func TestClearManagedProxyLeaseFailureQueuesRetryAndClearsAuth(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "proxy-secret")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260",
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Default"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	seedResinState(t, plugin, "grok-alice-old1", 0)
	result, errClear := plugin.ClearResinProxy(context.Background(), "auth-1")
	if errClear != nil {
		t.Fatal(errClear)
	}
	if !result.Cleared || !result.RemoteLeaseDeleteQueued {
		t.Fatalf("unexpected clear result: %#v", result)
	}
	if len(host.saves) != 1 {
		t.Fatalf("expected local auth clear save, got %d", len(host.saves))
	}
	if processed := plugin.processDueResinLeaseDeletes(context.Background(), time.Now().UTC()); processed != 1 {
		t.Fatalf("processed deletes = %d, want 1", processed)
	}
	state := plugin.store.Snapshot()
	if len(state.ResinLeaseDeletes) != 1 {
		t.Fatalf("failed delete was not retained for retry: %#v", state.ResinLeaseDeletes)
	}
	job := state.ResinLeaseDeletes[0]
	if job.Attempts != 1 || job.NextAttemptAt.IsZero() || job.LastError == "" {
		t.Fatalf("unexpected retry state: %#v", job)
	}
}

func TestManagedResinBindingPrefersProxyAccountOverStaleState(t *testing.T) {
	raw, errDecode := rawAuthToMap(makeAuthJSON(t, map[string]any{
		"proxy_url": "socks5h://Default.grok-alice-new1:proxy-secret@resin:2260",
	}))
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	account, managed := managedResinBinding(raw, AccountState{
		Resin: ResinState{Managed: true, Account: "grok-alice-old1"},
	}, ResinSettings{})
	if !managed || account != "grok-alice-new1" {
		t.Fatalf("managed binding = %q, %t; want proxy account", account, managed)
	}
}

func TestRotateResinKeepsAuthSwitchWhenStateWriteFails(t *testing.T) {
	oldProxy := "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260"
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": oldProxy,
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.LocalOnly = true
	})
	seedResinState(t, plugin, "grok-alice-old1", 0)
	failResinStateWrites(t, plugin)

	if errRotate := plugin.RotateResin(context.Background(), "auth-1"); errRotate != nil {
		t.Fatalf("state persistence failure made a committed rotation look failed: %v", errRotate)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	rotatedProxy := authRawString(saved, "proxy_url")
	if rotatedProxy == "" || rotatedProxy == oldProxy || strings.Contains(rotatedProxy, "grok-alice-old1") {
		t.Fatalf("auth did not keep the new proxy after state write failure: %q", rotatedProxy)
	}
	if len(host.saves) != 1 {
		t.Fatalf("expected one auth save, got %d", len(host.saves))
	}
}

func TestClearResinProxyKeepsAuthClearWhenStateWriteFails(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260",
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.LocalOnly = true
	})
	seedResinState(t, plugin, "grok-alice-old1", 0)
	failResinStateWrites(t, plugin)

	result, errClear := plugin.ClearResinProxy(context.Background(), "auth-1")
	if errClear != nil {
		t.Fatalf("state persistence failure made a committed clear look failed: %v", errClear)
	}
	if !result.Cleared || result.RemoteLeaseDeleteQueued || result.Error != "" {
		t.Fatalf("unexpected clear result after state write failure: %#v", result)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if authRawString(saved, "proxy_url") != "" {
		t.Fatalf("auth proxy_url was not cleared after state write failure: %q", authRawString(saved, "proxy_url"))
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
	seedResinState(t, plugin, "grok-alice-old1", 0)
	if errRotate := plugin.RotateResin(context.Background(), "auth-1"); errRotate != nil {
		t.Fatal(errRotate)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	stateAccount, okState := plugin.stateAccount("auth-1")
	if !okState || !stateAccount.Resin.Managed || stateAccount.Resin.Account == "" || stateAccount.Resin.Account == "grok-alice-old1" || stateAccount.Resin.Generation != 1 {
		t.Fatalf("unexpected local rotation state: %#v", stateAccount)
	}
	rotatedProxy := authRawString(saved, "proxy_url")
	if !strings.Contains(rotatedProxy, "Default."+stateAccount.Resin.Account) || !strings.Contains(rotatedProxy, "proxy-secret") {
		t.Fatalf("local rotation did not preserve platform and password: %q", rotatedProxy)
	}
	assertNoGuardMetadata(t, saved)
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
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.LocalOnly = true
	})
	seedResinState(t, plugin, "grok-alice-old1", 0)
	result, errClear := plugin.ClearResinProxy(context.Background(), "auth-1")
	if errClear != nil {
		t.Fatal(errClear)
	}
	if !result.Cleared || result.RemoteLeaseDeleteQueued {
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
	seedResinState(t, plugin, "grok-alice-old1", 0)
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
	stateAccount, okState := plugin.stateAccount("auth-1")
	if !okState || !stateAccount.Resin.Managed || stateAccount.Resin.Account == "" || stateAccount.Resin.Account == "grok-alice-old1" {
		t.Fatalf("unexpected auto-rotation state: %#v", stateAccount)
	}
	assertNoGuardMetadata(t, saved)
	if authRawString(saved, "access_token") != "access-auth-1" {
		t.Fatal("auto-rotation removed token fields")
	}
}

func TestTransparentResinRotationRetriesSameAuthWithNewProxy(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260",
	})
	host := newMockHostCall(entries, auths)
	host.scriptStreamSequence("auth-1", badStreamScript(), goodStreamScript())
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		enableTransparentForTest(cfg)
		cfg.AutoAction = "cooldown_only"
		cfg.Resin.Enabled = true
		cfg.Resin.LocalOnly = true
		cfg.Resin.AutoRotateAfterQualityFailure = true
	})
	seedResinState(t, plugin, "grok-alice-old1", 0)

	plugin.runTransparentStreamBridge(context.Background(), streamRequestForTest(), "plugin-stream-rotation-success")

	if len(host.modelCalls) != 2 {
		t.Fatalf("expected two upstream attempts, got %d", len(host.modelCalls))
	}
	firstProxy := host.modelCalls[0].ProxyURL
	secondProxy := host.modelCalls[1].ProxyURL
	if firstProxy == secondProxy {
		t.Fatalf("retry reused the old proxy: %q", firstProxy)
	}
	if !strings.Contains(secondProxy, "grok-email-") || strings.Contains(secondProxy, "grok-alice-old1") {
		t.Fatalf("retry did not use the rotated lease: %q", secondProxy)
	}
	account, okAccount := plugin.stateAccount("auth-1")
	if !okAccount || account.State != StateHealthy {
		t.Fatalf("account should stay healthy after rotated retry: %#v", account)
	}
	if len(host.saves) != 1 {
		t.Fatalf("expected only the rotation auth save, got %d", len(host.saves))
	}
	rotations := 0
	disables := 0
	for _, event := range plugin.stateStore().Snapshot().Events {
		switch event.Action {
		case "resin_rotate":
			rotations++
		case "guard_disable":
			disables++
		}
	}
	if rotations != 1 || disables != 0 {
		t.Fatalf("unexpected rotation/disable events: rotations=%d disables=%d", rotations, disables)
	}
}

func TestTransparentResinRotationCoolsAfterRetryFails(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260",
	})
	host := newMockHostCall(entries, auths)
	host.scriptStreamSequence("auth-1", badStreamScript(), badStreamScript())
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		enableTransparentForTest(cfg)
		cfg.AutoAction = "cooldown_only"
		cfg.Resin.Enabled = true
		cfg.Resin.LocalOnly = true
		cfg.Resin.AutoRotateAfterQualityFailure = true
	})
	seedResinState(t, plugin, "grok-alice-old1", 0)

	plugin.runTransparentStreamBridge(context.Background(), streamRequestForTest(), "plugin-stream-rotation-failure")

	if len(host.modelCalls) != 2 {
		t.Fatalf("expected two upstream attempts, got %d", len(host.modelCalls))
	}
	firstProxy := host.modelCalls[0].ProxyURL
	secondProxy := host.modelCalls[1].ProxyURL
	if firstProxy == secondProxy {
		t.Fatalf("retry reused the old proxy: %q", firstProxy)
	}
	account, okAccount := plugin.stateAccount("auth-1")
	if !okAccount || account.State != StateCooling || account.DisableKind != DisableKindCooldown {
		t.Fatalf("account was not cooled after rotated retry failed: %#v", account)
	}
	if account.CooldownUntil.IsZero() {
		t.Fatal("cooldown deadline was not set")
	}
	rotateIndex := -1
	disableIndex := -1
	events := plugin.stateStore().Snapshot().Events
	for index, event := range events {
		if event.Action == "resin_rotate" && rotateIndex < 0 {
			rotateIndex = index
		}
		if event.Action == "guard_disable" && disableIndex < 0 {
			disableIndex = index
		}
	}
	if rotateIndex < 0 || disableIndex < 0 || rotateIndex > disableIndex {
		t.Fatalf("rotation must precede cooldown: events=%#v", events)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if !authRawBool(saved, "disabled") {
		t.Fatal("cooldown did not persist disabled=true")
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

func TestRotateResinQueuesAdminAuthFailureForRetry(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	t.Setenv("RESIN_PROXY_TOKEN", "proxy-secret")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{
		"proxy_url": "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260",
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Default"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	seedResinState(t, plugin, "grok-alice-old1", 0)
	if errRotate := plugin.RotateResin(context.Background(), "auth-1"); errRotate != nil {
		t.Fatal(errRotate)
	}
	if len(host.saves) != 1 {
		t.Fatalf("expected one auth save, got %d", len(host.saves))
	}
	if processed := plugin.processDueResinLeaseDeletes(context.Background(), time.Now().UTC()); processed != 1 {
		t.Fatalf("processed deletes = %d, want 1", processed)
	}
	jobs := plugin.store.Snapshot().ResinLeaseDeletes
	if len(jobs) != 1 || jobs[0].Attempts != 1 || jobs[0].NextAttemptAt.IsZero() {
		t.Fatalf("admin auth failure was not retained for retry: %#v", jobs)
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
