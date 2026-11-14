package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestProbeFailureAfterStrikePermanentlyDisablesWithoutDeadlock(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{
		"disabled": true,
	})
	host := newMockHostCall(entries, auths)
	visible := strings.Repeat("degraded probe answer. ", 8) + "\\nQUALITY_OK"
	host.scriptStream("auth-1", []pluginapi.HostModelStreamReadResponse{
		streamData("data: {\"choices\":[{\"delta\":{\"content\":\"" + visible + "\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":100,\"completion_tokens_details\":{\"reasoning_tokens\":0}}}\n\n"),
		{Done: true},
	}...)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "cooldown_then_disable" })
	if errUpdate := plugin.stateStore().Update(func(state *State) error {
		account := state.Accounts["auth-1"]
		account.State = StateCooling
		account.DisableKind = DisableKindCooldown
		account.CooldownUntil = time.Now().UTC().Add(-time.Minute)
		account.QualityStrikeMissingThinking = true
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	done := make(chan error, 1)
	go func() {
		done <- plugin.ProbeAccount(context.Background(), "auth-1", true, "callback-1")
	}()
	select {
	case errProbe := <-done:
		if errProbe != nil {
			t.Fatal(errProbe)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ProbeAccount deadlocked or timed out")
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateDisabledByGuard || account.DisableKind != DisableKindPermanent {
		t.Fatalf("unexpected state after failed recovery probe: %#v", account)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if !authDisabled(saved) {
		t.Fatalf("auth was not permanently disabled: %#v", saved)
	}
	assertNoGuardMetadata(t, saved)
}

func TestProbeSuccessReenablesCooldownAndClearsStrikes(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{
		"disabled": true,
	})
	host := newMockHostCall(entries, auths)
	visible := strings.Repeat("reliable systems need careful design. ", 10) + "\\nQUALITY_OK"
	host.scriptStream("auth-1", []pluginapi.HostModelStreamReadResponse{
		streamData("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"plan carefully\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{\"content\":\"" + visible + "\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":120,\"completion_tokens_details\":{\"reasoning_tokens\":1}}}\n\n"),
		{Done: true},
	}...)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Quality.SoftTPS = 1e9
		cfg.Quality.HardTPS = 2e9
	})
	if errUpdate := plugin.stateStore().Update(func(state *State) error {
		account := state.Accounts["auth-1"]
		account.State = StateCooling
		account.DisableKind = DisableKindCooldown
		account.CooldownUntil = time.Now().UTC().Add(-time.Minute)
		account.QualityStrikeMissingThinking = true
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	if errProbe := plugin.ProbeAccount(context.Background(), "auth-1", true, "callback-1"); errProbe != nil {
		t.Fatal(errProbe)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateHealthy || account.QualityStrikeMissingThinking || account.SoftStrikes != 0 || account.ErrorStrikes != 0 || account.ThinkingStrikes != 0 {
		t.Fatalf("unexpected state after successful probe: %#v", account)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if authDisabled(saved) {
		t.Fatal("successful probe left auth disabled")
	}
	assertNoGuardMetadata(t, saved)
}

func TestProbeParsesHostLineChunksWithoutNewlines(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", []pluginapi.HostModelStreamReadResponse{
		streamData(": grok2api-reasoning-start"),
		streamData("event: response.reasoning_summary_text.delta"),
		streamData(`data: {"type":"response.reasoning_summary_text.delta","delta":"plan carefully"}`),
		streamData("event: response.output_text.delta"),
		streamData(`data: {"type":"response.output_text.delta","delta":"QUALITY_OK"}`),
		streamData("event: response.completed"),
		streamData(`data: {"response":{"usage":{"output_tokens":64,"output_tokens_details":{"reasoning_tokens":8}}}}`),
		{Done: true},
	}...)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Quality.SoftTPS = 1e9
		cfg.Quality.HardTPS = 2e9
	})
	if errProbe := plugin.ProbeAccount(context.Background(), "auth-1", true, ""); errProbe != nil {
		t.Fatal(errProbe)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateHealthy {
		t.Fatalf("newline-less host chunks were not treated as healthy: %#v", account)
	}
}

func TestProbeTPSIncludesPreStreamLatency(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	visible := strings.Repeat("reliable systems need careful design. ", 10) + "\\nQUALITY_OK"
	host.scriptStream("auth-1",
		streamData("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"plan carefully\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{\"content\":\""+visible+"\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":120,\"completion_tokens_details\":{\"reasoning_tokens\":1}}}\n\n"),
		pluginapi.HostModelStreamReadResponse{Done: true},
	)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Quality.SoftTPS = 1e9
		cfg.Quality.HardTPS = 2e9
	})

	innerHostCall := hostCall
	hostCall = func(method string, payload any) (json.RawMessage, error) {
		switch method {
		case pluginabi.MethodHostModelExecuteStream:
			time.Sleep(200 * time.Millisecond)
		case pluginabi.MethodHostModelStreamRead:
			time.Sleep(10 * time.Millisecond)
		}
		return innerHostCall(method, payload)
	}
	if errProbe := plugin.ProbeAccount(context.Background(), "auth-1", true, "callback-1"); errProbe != nil {
		t.Fatal(errProbe)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.Metrics.DurationMS < 180 || account.Metrics.FirstTokenMS < 180 {
		t.Fatalf("probe TPS clock started after the upstream wait: %#v", account.Metrics)
	}
	if account.Metrics.TPS <= 0 || account.Metrics.TPS > 500 {
		t.Fatalf("probe TPS = %.1f, want a sane grok2api-scale value: %#v", account.Metrics.TPS, account.Metrics)
	}
}
func TestProbeBodyUsesQualityMarkerProfilePrompt(t *testing.T) {
	body := probeBody(DefaultProbeConfig())
	var payload struct {
		Input []struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"input"`
	}
	if errUnmarshal := json.Unmarshal(body, &payload); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if len(payload.Input) != 1 || len(payload.Input[0].Content) != 1 {
		t.Fatalf("unexpected probe body: %s", body)
	}
	want := "先用三点总结为什么天空呈蓝色，最后一行只输出 QUALITY_OK。"
	if payload.Input[0].Content[0].Text != want {
		t.Fatalf("probe prompt = %q, want %q", payload.Input[0].Content[0].Text, want)
	}
}

func TestProbeBackoffGrowsAndCaps(t *testing.T) {
	cfg := DefaultProbeConfig()
	now := time.Now().UTC()
	first := nextProbeBackoff(cfg, AccountState{})
	if first != 15*time.Minute {
		t.Fatalf("first backoff = %s", first)
	}
	second := nextProbeBackoff(cfg, AccountState{LastProbeAt: now, ProbeBackoffUntil: now.Add(15 * time.Minute)})
	if second != 30*time.Minute {
		t.Fatalf("second backoff = %s", second)
	}
	capped := nextProbeBackoff(cfg, AccountState{LastProbeAt: now, ProbeBackoffUntil: now.Add(time.Hour)})
	if capped != time.Hour {
		t.Fatalf("capped backoff = %s", capped)
	}
}

func TestManagementProbeDoesNotReuseManagementCallback(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	visible := strings.Repeat("reliable systems need careful design. ", 10) + "\\nQUALITY_OK"
	host.scriptStream("auth-1", []pluginapi.HostModelStreamReadResponse{
		streamData("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"plan carefully\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{\"content\":\"" + visible + "\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":120,\"completion_tokens_details\":{\"reasoning_tokens\":1}}}\n\n"),
		{Done: true},
	}...)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Quality.SoftTPS = 1e9
		cfg.Quality.HardTPS = 2e9
	})
	response := actionManagementForTest(t, plugin, "accounts.probe", map[string]any{
		"auth_id": "auth-1",
		"force":   true,
	})
	if response.StatusCode != 200 {
		t.Fatalf("probe status = %d body=%s", response.StatusCode, response.Body)
	}
	host.mu.Lock()
	callbacks := append([]string(nil), host.callbacks...)
	host.mu.Unlock()
	if len(callbacks) != 1 || callbacks[0] != "" {
		t.Fatalf("management probe callbacks = %#v", callbacks)
	}
}

func TestProbeMarkerMissingRecordsQualityEventOnly(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", []pluginapi.HostModelStreamReadResponse{
		streamData("data: {\"choices\":[{\"delta\":{\"content\":\"wrong answer\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":4}}\n\n"),
		{Done: true},
	}...)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "observe" })
	if errProbe := plugin.ProbeAccount(context.Background(), "auth-1", true, ""); errProbe != nil {
		t.Fatal(errProbe)
	}
	state := plugin.store.Snapshot()
	internalErrors := 0
	qualityEvents := 0
	for _, event := range state.Events {
		if event.AuthID != "auth-1" {
			continue
		}
		if event.Action == "internal_error" {
			internalErrors++
		}
		if event.Class == ClassExpectedMarkerMissing && event.Action == "probe" && event.Result == "threshold_reached" {
			qualityEvents++
		}
	}
	if internalErrors != 0 {
		t.Fatalf("marker missing recorded %d internal errors", internalErrors)
	}
	if qualityEvents != 1 {
		t.Fatalf("marker missing quality events = %d, events=%#v", qualityEvents, state.Events)
	}
}

func TestProbeUnparsedStreamRecordsProbeErrorNotQualityMiss(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", []pluginapi.HostModelStreamReadResponse{
		streamData("not-an-sse-frame"),
		{Done: true},
	}...)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.AutoAction = "cooldown_then_disable"
		cfg.Quality.ConsecutiveErrors = 2
	})
	if errProbe := plugin.ProbeAccount(context.Background(), "auth-1", true, ""); errProbe != nil {
		t.Fatal(errProbe)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.ErrorStrikes != 1 || account.LastAttempt == nil || account.LastAttempt.Class != ClassProbeError {
		t.Fatalf("unparseable probe was not recorded as probe_error: %#v", account)
	}
	if account.State == StateCooling || account.State == StateDisabledByGuard {
		t.Fatalf("unparseable probe cooled account: %#v", account)
	}
	saved, errDecode := rawAuthToMap(host.auths["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if authDisabled(saved) {
		t.Fatal("unparseable probe disabled auth")
	}
}

func TestProbeTransportFailureRecordsInternalError(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "observe" })
	if errProbe := plugin.ProbeAccount(context.Background(), "auth-1", true, ""); errProbe != nil {
		t.Fatal(errProbe)
	}
	state := plugin.store.Snapshot()
	internalErrors := 0
	probeErrors := 0
	for _, event := range state.Events {
		if event.AuthID != "auth-1" {
			continue
		}
		if event.Action == "internal_error" {
			internalErrors++
		}
		if event.Class == ClassProbeError && event.Action == "probe" {
			probeErrors++
		}
	}
	if internalErrors != 1 {
		t.Fatalf("transport failure internal errors = %d, events=%#v", internalErrors, state.Events)
	}
	if probeErrors != 1 {
		t.Fatalf("transport failure probe events = %d, events=%#v", probeErrors, state.Events)
	}
}

func ageAccountsForProbe(t *testing.T, plugin *Plugin) {
	t.Helper()
	errUpdate := plugin.stateStore().Update(func(state *State) error {
		for _, account := range state.Accounts {
			if account == nil {
				continue
			}
			account.FirstSeenAt = time.Now().UTC().Add(-time.Minute)
			if account.State == "" || account.State == StateUnknown || account.State == StateExternallyDisabled {
				account.State = StateSuspect
				account.ProbeBackoffUntil = time.Time{}
			}
		}
		return nil
	})
	if errUpdate != nil {
		t.Fatal(errUpdate)
	}
}

func installStreamHold(entered chan<- string, release <-chan struct{}) {
	inner := hostCall
	hostCall = func(method string, payload any) (json.RawMessage, error) {
		if method == pluginabi.MethodHostModelExecuteStream {
			encoded, errMarshal := json.Marshal(payload)
			if errMarshal != nil {
				return nil, errMarshal
			}
			var req hostModelExecutionRequest
			if errUnmarshal := json.Unmarshal(encoded, &req); errUnmarshal != nil {
				return nil, errUnmarshal
			}
			entered <- req.AuthID
			<-release
		}
		return inner(method, payload)
	}
}

func waitForProbeEntries(t *testing.T, entered <-chan string, count int) []string {
	t.Helper()
	got := make([]string, 0, count)
	timeout := time.After(3 * time.Second)
	for len(got) < count {
		select {
		case id := <-entered:
			got = append(got, id)
		case <-timeout:
			t.Fatalf("started probes = %#v, want %d", got, count)
		}
	}
	return got
}

func assertNoProbeEntry(t *testing.T, entered <-chan string) {
	t.Helper()
	select {
	case id := <-entered:
		t.Fatalf("unexpected probe start for %s", id)
	case <-time.After(200 * time.Millisecond):
	}
}

func newDueProbeScheduler(t *testing.T, plugin *Plugin) *probeScheduler {
	t.Helper()
	scheduler := newProbeScheduler(plugin, context.Background())
	plugin.mu.RLock()
	policy := plugin.policy
	plugin.mu.RUnlock()
	scheduler.enqueueDue(plugin.catalogSnapshot(), plugin.stateStore().Snapshot(), policy, time.Now().UTC())
	scheduler.dispatch()
	return scheduler
}

func TestProbeConcurrencyRunsDistinctAccountsTogether(t *testing.T) {
	entries, auths := twoGrokAuths(t)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", pluginapi.HostModelStreamReadResponse{Done: true})
	host.scriptStream("auth-2", pluginapi.HostModelStreamReadResponse{Done: true})
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Probe.Concurrency = 2
	})
	ageAccountsForProbe(t, plugin)
	entered := make(chan string, 4)
	release := make(chan struct{})
	installStreamHold(entered, release)
	scheduler := newDueProbeScheduler(t, plugin)
	done := make(chan error, 1)
	go func() {
		scheduler.waitIdle()
		done <- nil
	}()
	got := waitForProbeEntries(t, entered, 2)
	if len(uniqueStrings(got)) != 2 {
		t.Fatalf("concurrent probes = %#v", got)
	}
	close(release)
	select {
	case errRun := <-done:
		if errRun != nil {
			t.Fatal(errRun)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("probe scheduler did not finish")
	}
}

func TestProbeConcurrencyLimitsParallelAccounts(t *testing.T) {
	entries, auths := twoGrokAuths(t)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", pluginapi.HostModelStreamReadResponse{Done: true})
	host.scriptStream("auth-2", pluginapi.HostModelStreamReadResponse{Done: true})
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Probe.Concurrency = 1
	})
	ageAccountsForProbe(t, plugin)
	entered := make(chan string, 4)
	release := make(chan struct{})
	installStreamHold(entered, release)
	scheduler := newDueProbeScheduler(t, plugin)
	done := make(chan error, 1)
	go func() {
		scheduler.waitIdle()
		done <- nil
	}()
	first := waitForProbeEntries(t, entered, 1)
	assertNoProbeEntry(t, entered)
	close(release)
	second := waitForProbeEntries(t, entered, 1)
	if first[0] == second[0] {
		t.Fatalf("serialized probes hit %s twice before the other account", first[0])
	}
	select {
	case errRun := <-done:
		if errRun != nil {
			t.Fatal(errRun)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("probe scheduler did not finish")
	}
}

func TestForceProbeTakesPriorityForNextSlot(t *testing.T) {
	plugin := NewPlugin()
	plugin.policy.Probe.Concurrency = 1
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if errAcquire := plugin.acquireProbeSlot(ctx, false); errAcquire != nil {
		t.Fatal(errAcquire)
	}

	priorityAcquired := make(chan struct{})
	releasePriority := make(chan struct{})
	go func() {
		if errAcquire := plugin.acquireProbeSlot(ctx, true); errAcquire == nil {
			close(priorityAcquired)
			<-releasePriority
			plugin.releaseProbeSlot()
		}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		plugin.probeGate.Lock()
		waiting := plugin.probePriorityWaiters
		plugin.probeGate.Unlock()
		if waiting == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("priority probe did not enter the wait queue")
		}
		time.Sleep(time.Millisecond)
	}

	normalAcquired := make(chan struct{})
	go func() {
		if errAcquire := plugin.acquireProbeSlot(ctx, false); errAcquire == nil {
			close(normalAcquired)
			plugin.releaseProbeSlot()
		}
	}()
	plugin.releaseProbeSlot()

	select {
	case <-priorityAcquired:
	case <-normalAcquired:
		t.Fatal("normal probe acquired the released slot before the priority probe")
	case <-ctx.Done():
		t.Fatal("priority probe did not acquire the next slot")
	}
	select {
	case <-normalAcquired:
		t.Fatal("normal probe started before the priority probe released its slot")
	case <-time.After(50 * time.Millisecond):
	}
	close(releasePriority)
	select {
	case <-normalAcquired:
	case <-ctx.Done():
		t.Fatal("normal probe did not acquire the slot after priority release")
	}
}
func TestProbeSameAccountDoesNotTakeSecondSlot(t *testing.T) {
	entries, auths := twoGrokAuths(t)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", pluginapi.HostModelStreamReadResponse{Done: true})
	host.scriptStream("auth-2", pluginapi.HostModelStreamReadResponse{Done: true})
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Probe.Concurrency = 2
	})
	entered := make(chan string, 4)
	release := make(chan struct{})
	installStreamHold(entered, release)
	errCh := make(chan error, 3)
	start := func(authID string) {
		go func() {
			errCh <- plugin.ProbeAccount(context.Background(), authID, true, "")
		}()
	}
	start("auth-1")
	start("auth-1")
	start("auth-2")
	got := waitForProbeEntries(t, entered, 2)
	seen := map[string]int{}
	for _, id := range got {
		seen[id]++
	}
	if seen["auth-1"] != 1 || seen["auth-2"] != 1 {
		t.Fatalf("in-flight probes = %#v, want one auth-1 and one auth-2", got)
	}
	assertNoProbeEntry(t, entered)
	close(release)
	replay := waitForProbeEntries(t, entered, 1)
	if replay[0] != "auth-1" {
		t.Fatalf("queued same-account probe started as %s", replay[0])
	}
	for i := 0; i < 3; i++ {
		select {
		case errProbe := <-errCh:
			if errProbe != nil {
				t.Fatal(errProbe)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("probe did not finish")
		}
	}
}

func TestProbeSchedulerFiltersHealthyAccountsBeforeQueue(t *testing.T) {
	entries, auths := twoGrokAuths(t)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	ageAccountsForProbe(t, plugin)
	if errUpdate := plugin.stateStore().Update(func(state *State) error {
		state.Accounts["auth-1"].State = StateHealthy
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}

	scheduler := newProbeScheduler(plugin, context.Background())
	plugin.mu.RLock()
	policy := plugin.policy
	plugin.mu.RUnlock()
	added := scheduler.enqueueDue(plugin.catalogSnapshot(), plugin.stateStore().Snapshot(), policy, time.Now().UTC())

	if added != 1 || len(scheduler.queue) != 1 || scheduler.queue[0] != "auth-2" {
		t.Fatalf("queued probes = %#v, added = %d, want only auth-2", scheduler.queue, added)
	}
}

func TestProbeSchedulerBoundsQueue(t *testing.T) {
	entries, auths := twoGrokAuths(t)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Probe.QueueSize = 1
	})
	ageAccountsForProbe(t, plugin)

	scheduler := newProbeScheduler(plugin, context.Background())
	plugin.mu.RLock()
	policy := plugin.policy
	plugin.mu.RUnlock()
	added := scheduler.enqueueDue(plugin.catalogSnapshot(), plugin.stateStore().Snapshot(), policy, time.Now().UTC())

	if added != 1 || len(scheduler.queue) != 1 {
		t.Fatalf("queue size = %d, added = %d, want bounded queue of 1", len(scheduler.queue), added)
	}
}

func TestProbeSchedulerDeduplicatesAccounts(t *testing.T) {
	entries, auths := twoGrokAuths(t)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	ageAccountsForProbe(t, plugin)

	scheduler := newProbeScheduler(plugin, context.Background())
	plugin.mu.RLock()
	policy := plugin.policy
	plugin.mu.RUnlock()
	state := plugin.stateStore().Snapshot()
	accounts := plugin.catalogSnapshot()
	first := scheduler.enqueueDue(accounts, state, policy, time.Now().UTC())
	second := scheduler.enqueueDue(accounts, state, policy, time.Now().UTC())

	if first != 2 || second != 0 || len(scheduler.queue) != 2 {
		t.Fatalf("first = %d, second = %d, queue = %#v", first, second, scheduler.queue)
	}
}

func TestProbeDueUnknownRespectsBackoff(t *testing.T) {
	now := time.Now().UTC()
	account := &AccountState{
		State:             StateUnknown,
		FirstSeenAt:       now.Add(-time.Hour),
		ProbeBackoffUntil: now.Add(time.Minute),
	}
	if probeDueForAccount(account, true, "auth-1", PolicyFromConfig(DefaultConfig()), now) {
		t.Fatal("unknown account with active probe backoff must not be due")
	}
}

func TestUnknownProbeJitterIsStableAndBounded(t *testing.T) {
	const max = 30 * time.Second
	first := unknownProbeJitter("auth-1", max)
	second := unknownProbeJitter("auth-1", max)
	if first != second {
		t.Fatalf("jitter is not stable: %s != %s", first, second)
	}
	if first < 0 || first >= max {
		t.Fatalf("jitter = %s, want [0, %s)", first, max)
	}
	if unknownProbeJitter("auth-1", 0) != 0 {
		t.Fatal("disabled jitter must be zero")
	}
}
func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
