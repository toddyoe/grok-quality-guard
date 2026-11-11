package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStateStorePersistsAndReloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "state.json")
	store := NewStateStore(path)
	policy := PolicyFromConfig(DefaultConfig())
	if errLoad := store.Load(policy, "revision-1"); errLoad != nil {
		t.Fatal(errLoad)
	}
	if errUpdate := store.Update(func(state *State) error {
		state.Accounts["auth-1"] = &AccountState{
			AuthID:        "auth-1",
			State:         StateCooling,
			FirstSeenAt:   time.Now().UTC(),
			CooldownUntil: time.Now().UTC().Add(time.Hour),
		}
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	raw, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	var persisted State
	if errUnmarshal := json.Unmarshal(raw, &persisted); errUnmarshal != nil {
		t.Fatalf("state file is not valid JSON: %v", errUnmarshal)
	}
	if persisted.SchemaVersion != stateSchemaVersion || persisted.Accounts["auth-1"] == nil {
		t.Fatalf("unexpected persisted state: %#v", persisted)
	}
	reloaded := NewStateStore(path)
	if errLoad := reloaded.Load(policy, "revision-2"); errLoad != nil {
		t.Fatal(errLoad)
	}
	snapshot := reloaded.Snapshot()
	account := snapshot.Accounts["auth-1"]
	if account == nil || account.State != StateCooling || account.CooldownUntil.IsZero() {
		t.Fatalf("reloaded account mismatch: %#v", account)
	}
	matches, errGlob := filepath.Glob(filepath.Join(filepath.Dir(path), ".state-*.tmp"))
	if errGlob != nil {
		t.Fatal(errGlob)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary state files were left behind: %#v", matches)
	}
}

func TestStateStoreTrimsEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store := NewStateStore(path)
	policy := PolicyFromConfig(DefaultConfig())
	policy.EventsLimit = 3
	if errLoad := store.Load(policy, "revision"); errLoad != nil {
		t.Fatal(errLoad)
	}
	for i := 0; i < 10; i++ {
		if errUpdate := store.Update(func(state *State) error {
			appendEvent(state, Event{Time: time.Now().UTC(), Action: "test"})
			return nil
		}); errUpdate != nil {
			t.Fatal(errUpdate)
		}
	}
	state := store.Snapshot()
	if len(state.Events) != 3 {
		t.Fatalf("event count = %d, want 3", len(state.Events))
	}
}

func TestStateStoreConcurrentUpdatesRemainConsistent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store := NewStateStore(path)
	if errLoad := store.Load(PolicyFromConfig(DefaultConfig()), "revision"); errLoad != nil {
		t.Fatal(errLoad)
	}
	var wait sync.WaitGroup
	for i := 0; i < 32; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_ = store.Update(func(state *State) error {
				appendEvent(state, Event{Time: time.Now().UTC(), AuthID: "auth", Action: "concurrent", Detail: "index"})
				return nil
			})
		}(i)
	}
	wait.Wait()
	state := store.Snapshot()
	if len(state.Events) != 32 {
		t.Fatalf("event count = %d, want 32", len(state.Events))
	}
	raw, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	var persisted State
	if errUnmarshal := json.Unmarshal(raw, &persisted); errUnmarshal != nil {
		t.Fatalf("state file corrupted: %v", errUnmarshal)
	}
}
func TestStateStoreUpdateIfChangedSkipsNoopWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store := NewStateStore(path)
	if errLoad := store.Load(PolicyFromConfig(DefaultConfig()), "revision"); errLoad != nil {
		t.Fatal(errLoad)
	}
	before := store.Snapshot().UpdatedAt
	time.Sleep(2 * time.Millisecond)
	if errUpdate := store.UpdateIfChanged(func(*State) error { return nil }); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	after := store.Snapshot().UpdatedAt
	if !after.Equal(before) {
		t.Fatalf("no-op update changed UpdatedAt from %s to %s", before, after)
	}
}

func TestStateStorePersistsResinSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store := NewStateStore(path)
	if errLoad := store.Load(PolicyFromConfig(DefaultConfig()), "revision"); errLoad != nil {
		t.Fatal(errLoad)
	}
	validatedAt := time.Now().UTC().Truncate(time.Second)
	if errUpdate := store.Update(func(state *State) error {
		state.Resin = ResinSettings{
			Configured:                    true,
			Enabled:                       true,
			Platform:                      "Default",
			PlatformID:                    testResinPlatformID,
			AdminURL:                      "http://resin.test",
			ProxyTemplate:                 "socks5h://{platform}.{account}:{proxy_token}@resin:2260",
			AutoRotateAfterQualityFailure: true,
			AdminToken:                    "admin-secret",
			ProxyToken:                    "proxy-secret",
			ValidatedAt:                   validatedAt,
		}
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	reloaded := NewStateStore(path)
	if errLoad := reloaded.Load(PolicyFromConfig(DefaultConfig()), "revision"); errLoad != nil {
		t.Fatal(errLoad)
	}
	settings := reloaded.Snapshot().Resin
	if !settings.Configured || settings.AdminToken != "admin-secret" || settings.ProxyToken != "proxy-secret" {
		t.Fatalf("reloaded resin settings mismatch: %#v", settings)
	}
	if !settings.ValidatedAt.Equal(validatedAt) {
		t.Fatalf("validated_at mismatch: got %s want %s", settings.ValidatedAt, validatedAt)
	}
}
