package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func manyGrokAuths(t *testing.T, count int) []pluginapi.HostAuthFileEntry {
	t.Helper()
	entries := make([]pluginapi.HostAuthFileEntry, 0, count)
	for index := 0; index < count; index++ {
		authID := fmt.Sprintf("auth-%02d", index+1)
		entry, _ := singleGrokAuth(t, authID, authID+"@example.com", nil)
		entries = append(entries, entry...)
	}
	return entries
}

func selectionAuthRaw(t *testing.T, entries []pluginapi.HostAuthFileEntry) map[string]json.RawMessage {
	t.Helper()
	auths := make(map[string]json.RawMessage, len(entries))
	for _, entry := range entries {
		auths[entry.ID] = makeAuthJSON(t, map[string]any{
			"type":          "xai",
			"email":         entry.Email,
			"access_token":  "access-" + entry.ID,
			"refresh_token": "refresh-" + entry.ID,
			"disabled":      false,
		})
	}
	return auths
}

func setSelectionStates(t *testing.T, plugin *Plugin, states map[string]string) {
	t.Helper()
	if errUpdate := plugin.stateStore().UpdateIfChanged(func(state *State) error {
		for authID, value := range states {
			plugin.ensureStateAccount(state, authID, "", "").State = value
		}
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	if errRebuild := plugin.rebuildSelectionIndex(); errRebuild != nil {
		t.Fatal(errRebuild)
	}
}

func selectionRequestWithSession(sessionID string) rpcExecutorRequest {
	request := streamRequestForTest()
	request.Headers = http.Header{"Content-Type": []string{"application/json"}}
	if sessionID != "" {
		request.Headers.Set("X-Session-Id", sessionID)
	}
	return request
}

func nextSelectionAttempt(t *testing.T, plugin *Plugin, sessionID string) (*transparentSelection, transparentCandidateAttempt) {
	t.Helper()
	request := selectionRequestWithSession(sessionID)
	selection, errSelection := plugin.newTransparentSelection(request, request.OriginalRequest, request.SourceFormat)
	if errSelection != nil {
		t.Fatal(errSelection)
	}
	attempt, ok := selection.Next(context.Background())
	if !ok {
		t.Fatal("selection returned no candidate")
	}
	return selection, attempt
}

func TestSelectionIndexOrdersHealthyThenUnknownAndPriorityDescending(t *testing.T) {
	accounts := []Account{
		{ID: "healthy-low", Provider: "xai", Priority: 1},
		{ID: "healthy-high", Provider: "xai", Priority: 9},
		{ID: "unknown-high", Provider: "xai", Priority: 100},
		{ID: "suspect", Provider: "xai", Priority: 1000},
	}
	states := map[string]*AccountState{
		"healthy-low":  {State: StateHealthy},
		"healthy-high": {State: StateHealthy},
		"unknown-high": {State: StateUnknown},
		"suspect":      {State: StateSuspect},
	}
	var index routingSelectionIndex
	index.Build(accounts, states)

	order := index.Order()
	want := []string{"0:9", "0:1", "1:100"}
	if len(order) != len(want) {
		t.Fatalf("layer order = %#v, want %#v", order, want)
	}
	for position := range want {
		if order[position] != want[position] {
			t.Fatalf("layer order = %#v, want %#v", order, want)
		}
	}
	if index.Total() != 3 {
		t.Fatalf("candidate total = %d, want 3", index.Total())
	}
	if layer, ok := index.LayerForAuth("unknown-high"); !ok || layer != "1:100" {
		t.Fatalf("unknown account layer = %q, %v", layer, ok)
	}
}

func TestTransparentSelectionRoundRobinDistributesConcurrentIdleAuths(t *testing.T) {
	entries := manyGrokAuths(t, 10)
	host := newMockHostCall(entries, selectionAuthRaw(t, entries))
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.TransparentGuard = true
		cfg.Routing.SelectionStrategy = "round_robin"
		cfg.Routing.PreferLastSuccess = false
		cfg.Routing.SessionAffinity = false
	})
	states := make(map[string]string, len(entries))
	for _, entry := range entries {
		states[entry.ID] = StateHealthy
	}
	setSelectionStates(t, plugin, states)

	const requests = 10
	start := make(chan struct{})
	results := make(chan transparentCandidateAttempt, requests)
	errs := make(chan error, requests)
	for index := 0; index < requests; index++ {
		go func() {
			<-start
			request := selectionRequestWithSession("")
			selection, errSelection := plugin.newTransparentSelection(request, request.OriginalRequest, request.SourceFormat)
			if errSelection != nil {
				errs <- errSelection
				return
			}
			attempt, ok := selection.Next(context.Background())
			if !ok {
				errs <- fmt.Errorf("selection returned no candidate")
				return
			}
			results <- attempt
		}()
	}
	close(start)

	seen := make(map[string]int, requests)
	attempts := make([]transparentCandidateAttempt, 0, requests)
	for len(attempts) < requests {
		select {
		case errSelection := <-errs:
			t.Fatal(errSelection)
		case attempt := <-results:
			seen[attempt.Account.ID]++
			attempts = append(attempts, attempt)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for concurrent selections")
		}
	}
	for _, attempt := range attempts {
		attempt.Release()
	}
	if len(seen) != requests {
		t.Fatalf("concurrent first attempts used %d distinct auths, want %d: %#v", len(seen), requests, seen)
	}
	for authID, count := range seen {
		if count != 1 {
			t.Fatalf("auth %s was selected %d times: %#v", authID, count, seen)
		}
	}
}

func TestTransparentSelectionPreferredBusyFallsBackToIdleAuth(t *testing.T) {
	entries := manyGrokAuths(t, 2)
	host := newMockHostCall(entries, selectionAuthRaw(t, entries))
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.TransparentGuard = true
		cfg.Routing.SelectionStrategy = "fill_first"
		cfg.Routing.PreferLastSuccess = true
		cfg.Routing.SessionAffinity = false
	})
	setSelectionStates(t, plugin, map[string]string{"auth-01": StateHealthy, "auth-02": StateHealthy})

	firstSelection, firstAttempt := nextSelectionAttempt(t, plugin, "")
	if firstAttempt.Account.ID != "auth-01" {
		t.Fatalf("first auth = %s, want auth-01", firstAttempt.Account.ID)
	}
	firstSelection.MarkSuccess(firstAttempt.Account.ID, firstAttempt.Source)
	firstAttempt.Release()

	_, secondAttempt := nextSelectionAttempt(t, plugin, "")
	if secondAttempt.Account.ID != "auth-01" || secondAttempt.Source != transparentSourcePreferred {
		t.Fatalf("second attempt = %#v, want idle preferred auth-01", secondAttempt)
	}

	thirdSelection, thirdAttempt := nextSelectionAttempt(t, plugin, "")
	if thirdAttempt.Account.ID != "auth-02" {
		t.Fatalf("busy preferred was reused before idle auth: %#v", thirdAttempt)
	}
	if preferred, _ := plugin.selectionRuntime.Preferred(); preferred != "auth-01" {
		t.Fatalf("busy preferred changed to %q", preferred)
	}
	thirdSelection.MarkSuccess(thirdAttempt.Account.ID, thirdAttempt.Source)
	secondAttempt.Release()
	thirdAttempt.Release()

	fourthSelection, fourthAttempt := nextSelectionAttempt(t, plugin, "")
	if fourthAttempt.Account.ID != "auth-01" || fourthAttempt.Source != transparentSourcePreferred {
		t.Fatalf("idle preferred was not restored: %#v", fourthAttempt)
	}
	fourthSelection.MarkSuccess(fourthAttempt.Account.ID, fourthAttempt.Source)
	fourthAttempt.Release()
}

func TestTransparentSelectionConcurrentFirstSessionRequestsShareAuth(t *testing.T) {
	entries := manyGrokAuths(t, 2)
	host := newMockHostCall(entries, selectionAuthRaw(t, entries))
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.TransparentGuard = true
		cfg.Routing.SelectionStrategy = "round_robin"
		cfg.Routing.PreferLastSuccess = false
		cfg.Routing.SessionAffinity = true
	})
	setSelectionStates(t, plugin, map[string]string{"auth-01": StateHealthy, "auth-02": StateHealthy})

	const requests = 2
	start := make(chan struct{})
	results := make(chan transparentCandidateAttempt, requests)
	for index := 0; index < requests; index++ {
		go func() {
			<-start
			request := selectionRequestWithSession("session-shared")
			selection, errSelection := plugin.newTransparentSelection(request, request.OriginalRequest, request.SourceFormat)
			if errSelection != nil {
				results <- transparentCandidateAttempt{}
				return
			}
			attempt, _ := selection.Next(context.Background())
			results <- attempt
		}()
	}
	close(start)
	first := <-results
	second := <-results
	if first.Account.ID == "" || second.Account.ID == "" {
		t.Fatal("session selection returned an empty auth")
	}
	if first.Account.ID != second.Account.ID {
		t.Fatalf("concurrent first session requests used different auths: %s vs %s", first.Account.ID, second.Account.ID)
	}
	first.Release()
	second.Release()
	request := selectionRequestWithSession("session-shared")
	sessionKey := plugin.selectionSessionKey(request, request.OriginalRequest, request.SourceFormat)
	if bound, ok := plugin.selectionRuntime.LookupBinding(sessionKey, time.Now().UTC()); !ok || bound != first.Account.ID {
		t.Fatalf("session binding = %q, %v, want %s", bound, ok, first.Account.ID)
	}
}

func TestUnknownToHealthyMovesAuthToHealthyLayerWithoutFullRebuild(t *testing.T) {
	entries := manyGrokAuths(t, 2)
	host := newMockHostCall(entries, selectionAuthRaw(t, entries))
	plugin := newTestPlugin(t, host, nil)
	setSelectionStates(t, plugin, map[string]string{"auth-01": StateHealthy, "auth-02": StateUnknown})
	beforeGeneration := plugin.selectionIndex.BuildGeneration()

	if errUpdate := plugin.updateStateAccount("auth-02", func(account *AccountState) error {
		account.State = StateHealthy
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}

	layer, ok := plugin.selectionIndex.LayerForAuth("auth-02")
	if !ok || layer != "0:1" {
		t.Fatalf("unknown account did not move to healthy layer: %q, %v", layer, ok)
	}
	if generation := plugin.selectionIndex.BuildGeneration(); generation != beforeGeneration {
		t.Fatalf("account state move rebuilt the full index: before=%d after=%d", beforeGeneration, generation)
	}
}

func TestRecordAttemptErrorDoesNotInvalidateSelectionIndex(t *testing.T) {
	entries := manyGrokAuths(t, 1)
	host := newMockHostCall(entries, selectionAuthRaw(t, entries))
	plugin := newTestPlugin(t, host, nil)
	setSelectionStates(t, plugin, map[string]string{"auth-01": StateHealthy})
	beforeGeneration := plugin.selectionIndex.BuildGeneration()

	if errEvent := plugin.recordAttemptError("auth-01", ClassIgnored, fmt.Errorf("temporary upstream capacity")); errEvent != nil {
		t.Fatal(errEvent)
	}
	request := selectionRequestWithSession("")
	if _, errSelection := plugin.newTransparentSelection(request, request.OriginalRequest, request.SourceFormat); errSelection != nil {
		t.Fatal(errSelection)
	}
	if generation := plugin.selectionIndex.BuildGeneration(); generation != beforeGeneration {
		t.Fatalf("event write rebuilt the selection index: before=%d after=%d", beforeGeneration, generation)
	}
}

func TestRoutingPolicyUpdateHotReloadsSelectionConfig(t *testing.T) {
	entries := manyGrokAuths(t, 1)
	host := newMockHostCall(entries, selectionAuthRaw(t, entries))
	plugin := newTestPlugin(t, host, nil)
	policy := PolicyFromConfig(DefaultConfig())
	policy.Routing.SelectionStrategy = "fill_first"
	policy.Routing.PreferLastSuccess = false
	policy.Routing.SessionAffinity = false
	policy.Routing.SessionAffinityTTL = NewDuration(2 * time.Hour)
	policy.Routing.SessionAffinityMaxSize = 77

	_, status, errUpdate := plugin.actionPolicyUpdate(map[string]any{"policy": policy})
	if errUpdate != nil || status != http.StatusOK {
		t.Fatalf("policy update status=%d err=%v", status, errUpdate)
	}
	if plugin.cfg.Routing.SelectionStrategy != "fill_first" || plugin.cfg.Routing.PreferLastSuccess || plugin.cfg.Routing.SessionAffinity {
		t.Fatalf("routing config was not hot-reloaded: %#v", plugin.cfg.Routing)
	}
	if plugin.cfg.Routing.SessionAffinityTTL.Duration != 2*time.Hour || plugin.cfg.Routing.SessionAffinityMaxSize != 77 {
		t.Fatalf("routing TTL config was not hot-reloaded: %#v", plugin.cfg.Routing)
	}
}

func TestSelectionRuntimeBindingTTLAndMaxEntries(t *testing.T) {
	runtime := newRoutingSelectionRuntime()
	now := time.Now().UTC()
	if bound := runtime.GetOrBind("session-a", "auth-a", now, time.Minute, 2); bound != "auth-a" {
		t.Fatalf("initial binding = %q", bound)
	}
	if bound := runtime.GetOrBind("session-b", "auth-b", now.Add(time.Second), time.Minute, 2); bound != "auth-b" {
		t.Fatalf("second binding = %q", bound)
	}
	if bound := runtime.GetOrBind("session-c", "auth-c", now.Add(2*time.Second), time.Minute, 2); bound != "auth-c" {
		t.Fatalf("third binding = %q", bound)
	}
	if _, ok := runtime.LookupBinding("session-a", now.Add(3*time.Second)); ok {
		t.Fatal("oldest binding was not evicted at max entries")
	}
	if _, ok := runtime.LookupBinding("session-c", now.Add(2*time.Minute)); ok {
		t.Fatal("expired binding was not removed")
	}
}

func TestSelectionRuntimeRoundRobinCursorAdvancesExactlyOnce(t *testing.T) {
	runtime := newRoutingSelectionRuntime()
	const goroutines = 200
	counts := make([]int, 10)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for index := 0; index < goroutines; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := runtime.StrategyStart("layer", "round_robin", len(counts))
			mu.Lock()
			counts[start]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	for index, count := range counts {
		if count != goroutines/len(counts) {
			t.Fatalf("round-robin bucket %d = %d, want %d: %#v", index, count, goroutines/len(counts), counts)
		}
	}
}
func TestTransparentSelectionRebindsSessionAfterAuthBecomesIneligible(t *testing.T) {
	entries := manyGrokAuths(t, 2)
	host := newMockHostCall(entries, selectionAuthRaw(t, entries))
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.TransparentGuard = true
		cfg.Routing.SelectionStrategy = "fill_first"
		cfg.Routing.PreferLastSuccess = false
		cfg.Routing.SessionAffinity = true
	})
	setSelectionStates(t, plugin, map[string]string{"auth-01": StateHealthy, "auth-02": StateHealthy})

	firstSelection, firstAttempt := nextSelectionAttempt(t, plugin, "session-rebind")
	if firstAttempt.Account.ID != "auth-01" {
		t.Fatalf("first auth = %s, want auth-01", firstAttempt.Account.ID)
	}
	firstSelection.MarkSuccess(firstAttempt.Account.ID, firstAttempt.Source)
	firstAttempt.Release()
	if errUpdate := plugin.updateStateAccount("auth-01", func(account *AccountState) error {
		account.State = StateSuspect
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}

	secondSelection, secondAttempt := nextSelectionAttempt(t, plugin, "session-rebind")
	if secondAttempt.Account.ID != "auth-02" {
		t.Fatalf("session did not rebind after auth invalidation: %#v", secondAttempt)
	}
	secondSelection.MarkSuccess(secondAttempt.Account.ID, secondAttempt.Source)
	secondAttempt.Release()
}

func TestSelectionSessionKeySeparatesCallerScopeAndSession(t *testing.T) {
	first := selectionRequestWithSession("session-a")
	first.Headers.Set("Authorization", "Bearer caller-a")
	second := selectionRequestWithSession("session-a")
	second.Headers.Set("Authorization", "Bearer caller-b")
	third := selectionRequestWithSession("session-b")
	third.Headers.Set("Authorization", "Bearer caller-a")

	firstKey := (&Plugin{}).selectionSessionKey(first, first.OriginalRequest, first.SourceFormat)
	secondKey := (&Plugin{}).selectionSessionKey(second, second.OriginalRequest, second.SourceFormat)
	thirdKey := (&Plugin{}).selectionSessionKey(third, third.OriginalRequest, third.SourceFormat)
	if firstKey == "" || secondKey == "" || thirdKey == "" {
		t.Fatal("session key should not be empty for stable session headers")
	}
	if firstKey == secondKey {
		t.Fatal("different caller scopes reused the same session key")
	}
	if firstKey == thirdKey {
		t.Fatal("different sessions reused the same session key")
	}
}
