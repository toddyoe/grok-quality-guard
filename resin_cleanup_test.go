package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestResinLeaseDeleteBackoff(t *testing.T) {
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{attempts: 0, want: 30 * time.Second},
		{attempts: 1, want: 30 * time.Second},
		{attempts: 2, want: time.Minute},
		{attempts: 3, want: 2 * time.Minute},
		{attempts: 8, want: time.Hour},
		{attempts: 100, want: time.Hour},
	}
	for _, tc := range cases {
		if got := resinLeaseDeleteBackoff(tc.attempts); got != tc.want {
			t.Fatalf("backoff(%d) = %s, want %s", tc.attempts, got, tc.want)
		}
	}
}

func TestCatalogRemovalQueuesManagedResinLeaseDelete(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	var deleted atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if request.Header.Get("Authorization") != "Bearer admin-secret" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		if request.URL.Path != "/api/v1/platforms/"+testResinPlatformID+"/leases/grok-alice-old1" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		deleted.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Default"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	if errUpdate := plugin.updateStateAccount("auth-1", func(account *AccountState) error {
		account.Resin = ResinState{Managed: true, Account: "grok-alice-old1", Generation: 1}
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}

	host.mu.Lock()
	host.entries = nil
	host.mu.Unlock()
	if errRefresh := plugin.RefreshCatalog(context.Background()); errRefresh != nil {
		t.Fatal(errRefresh)
	}

	state := plugin.store.Snapshot()
	if _, exists := state.Accounts["auth-1"]; exists {
		t.Fatalf("removed auth remained in state: %#v", state.Accounts["auth-1"])
	}
	if len(state.ResinLeaseDeletes) != 1 {
		t.Fatalf("queued lease deletes = %d, want 1: %#v", len(state.ResinLeaseDeletes), state.ResinLeaseDeletes)
	}
	job := state.ResinLeaseDeletes[0]
	if job.AuthID != "auth-1" || job.Account != "grok-alice-old1" || job.AdminURL != server.URL || job.PlatformID != testResinPlatformID {
		t.Fatalf("unexpected lease delete job: %#v", job)
	}
	if deleted.Load() != 0 {
		t.Fatalf("catalog refresh deleted the lease synchronously")
	}
	if processed := plugin.processDueResinLeaseDeletes(context.Background(), time.Now().UTC()); processed != 1 {
		t.Fatalf("processed lease deletes = %d, want 1", processed)
	}
	if deleted.Load() != 1 {
		t.Fatalf("resin delete calls = %d, want 1", deleted.Load())
	}
	if jobs := plugin.store.Snapshot().ResinLeaseDeletes; len(jobs) != 0 {
		t.Fatalf("successful lease delete remained queued: %#v", jobs)
	}
}

func TestCatalogRemovalSkipsResinLeaseDeleteWithoutAPIConfig(t *testing.T) {
	tests := []struct {
		name       string
		disabled   bool
		localOnly  bool
		clearToken bool
		resin      ResinState
	}{
		{name: "local only", localOnly: true, resin: ResinState{Managed: true, Account: "grok-alice-old1"}},
		{name: "disabled", disabled: true, resin: ResinState{Managed: true, Account: "grok-alice-old1"}},
		{name: "missing token", clearToken: true, resin: ResinState{Managed: true, Account: "grok-alice-old1"}},
		{name: "unmanaged", resin: ResinState{}},
		{name: "missing account", resin: ResinState{Managed: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.clearToken {
				t.Setenv("RESIN_ADMIN_TOKEN", "")
			} else {
				t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
			}
			entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
			host := newMockHostCall(entries, auths)
			plugin := newTestPlugin(t, host, func(cfg *Config) {
				cfg.Resin.Enabled = !tc.disabled
				cfg.Resin.LocalOnly = tc.localOnly
				cfg.Resin.Platform = "Default"
				cfg.Resin.PlatformID = testResinPlatformID
				cfg.Resin.AdminURL = "http://resin.test"
			})
			if errUpdate := plugin.updateStateAccount("auth-1", func(account *AccountState) error {
				account.Resin = tc.resin
				return nil
			}); errUpdate != nil {
				t.Fatal(errUpdate)
			}

			host.mu.Lock()
			host.entries = nil
			host.mu.Unlock()
			if errRefresh := plugin.RefreshCatalog(context.Background()); errRefresh != nil {
				t.Fatal(errRefresh)
			}

			state := plugin.store.Snapshot()
			if _, exists := state.Accounts["auth-1"]; exists {
				t.Fatalf("removed auth remained in state: %#v", state.Accounts["auth-1"])
			}
			if len(state.ResinLeaseDeletes) != 0 {
				t.Fatalf("unexpected queued lease deletes: %#v", state.ResinLeaseDeletes)
			}
		})
	}
}

func TestResinLeaseDeleteRetriesAfterBackoffAndDeduplicates(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "admin-secret")
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		switch requests.Add(1) {
		case 1, 2:
			writer.WriteHeader(http.StatusInternalServerError)
		default:
			writer.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.Platform = "Default"
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = server.URL
	})
	settings := plugin.ResinConfig()
	added, errQueue := plugin.enqueueResinLeaseDelete("auth-1", settings, "grok-alice-old1")
	if errQueue != nil || !added {
		t.Fatalf("enqueue delete: added=%t err=%v", added, errQueue)
	}
	added, errQueue = plugin.enqueueResinLeaseDelete("auth-1", settings, "grok-alice-old1")
	if errQueue != nil || added {
		t.Fatalf("duplicate enqueue: added=%t err=%v", added, errQueue)
	}
	if jobs := plugin.store.Snapshot().ResinLeaseDeletes; len(jobs) != 1 {
		t.Fatalf("queued jobs = %d, want 1", len(jobs))
	}

	now := time.Now().UTC()
	if processed := plugin.processDueResinLeaseDeletes(context.Background(), now); processed != 1 {
		t.Fatalf("first processed = %d, want 1", processed)
	}
	job := plugin.store.Snapshot().ResinLeaseDeletes[0]
	if job.Attempts != 1 || !job.NextAttemptAt.After(now) {
		t.Fatalf("first retry was not scheduled: %#v", job)
	}
	if processed := plugin.processDueResinLeaseDeletes(context.Background(), now.Add(time.Second)); processed != 0 {
		t.Fatalf("early retry processed = %d, want 0", processed)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("early retry sent %d requests, want 1", got)
	}

	if processed := plugin.processDueResinLeaseDeletes(context.Background(), job.NextAttemptAt.Add(time.Second)); processed != 1 {
		t.Fatalf("second processed = %d, want 1", processed)
	}
	job = plugin.store.Snapshot().ResinLeaseDeletes[0]
	if job.Attempts != 2 || !job.NextAttemptAt.After(time.Now().UTC()) {
		t.Fatalf("second retry was not scheduled: %#v", job)
	}
	if processed := plugin.processDueResinLeaseDeletes(context.Background(), job.NextAttemptAt.Add(time.Second)); processed != 1 {
		t.Fatalf("third processed = %d, want 1", processed)
	}
	if jobs := plugin.store.Snapshot().ResinLeaseDeletes; len(jobs) != 0 {
		t.Fatalf("successful retry left jobs behind: %#v", jobs)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("delete requests = %d, want 3", got)
	}
}

func TestResinLeaseDeleteUsesStoredTargetWhenConfigChanges(t *testing.T) {
	t.Setenv("RESIN_ADMIN_TOKEN", "new-token")
	var oldCalls atomic.Int32
	var newCalls atomic.Int32
	oldServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		oldCalls.Add(1)
		if request.Header.Get("Authorization") != "Bearer old-token" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer oldServer.Close()
	newServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		newCalls.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer newServer.Close()

	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Resin.Enabled = true
		cfg.Resin.PlatformID = testResinPlatformID
		cfg.Resin.AdminURL = newServer.URL
	})
	settings := ResinSettings{
		AdminURL:   oldServer.URL,
		AdminToken: "old-token",
		PlatformID: testResinPlatformID,
	}
	added, errQueue := plugin.enqueueResinLeaseDelete("auth-1", settings, "grok-alice-old1")
	if errQueue != nil || !added {
		t.Fatalf("enqueue delete: added=%t err=%v", added, errQueue)
	}
	if processed := plugin.processDueResinLeaseDeletes(context.Background(), time.Now().UTC()); processed != 1 {
		t.Fatalf("processed deletes = %d, want 1", processed)
	}
	if oldCalls.Load() != 1 || newCalls.Load() != 0 {
		t.Fatalf("delete target calls: old=%d new=%d", oldCalls.Load(), newCalls.Load())
	}
	if jobs := plugin.store.Snapshot().ResinLeaseDeletes; len(jobs) != 0 {
		t.Fatalf("stored target delete was not removed: %#v", jobs)
	}
}
