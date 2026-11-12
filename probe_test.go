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
		"grok_quality_guard": map[string]any{
			"managed":                 true,
			"disable_kind":            DisableKindCooldown,
			"cooldown_until":          "2020-01-01T00:00:00Z",
			"strike_missing_thinking": true,
		},
	})
	host := newMockHostCall(entries, auths)
	visible := strings.Repeat("degraded probe answer. ", 8) + "\\nQUALITY_OK"
	host.scriptStream("auth-1", []pluginapi.HostModelStreamReadResponse{
		streamData("data: {\"choices\":[{\"delta\":{\"content\":\"" + visible + "\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":100,\"completion_tokens_details\":{\"reasoning_tokens\":0}}}\n\n"),
		{Done: true},
	}...)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "cooldown_then_disable" })
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
	if !authDisabled(saved) || guardMeta(saved).DisableKind != DisableKindPermanent {
		t.Fatalf("auth was not permanently disabled: %#v", saved)
	}
}

func TestProbeSuccessReenablesCooldownAndClearsStrikes(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{
		"disabled": true,
		"grok_quality_guard": map[string]any{
			"managed":                 true,
			"disable_kind":            DisableKindCooldown,
			"cooldown_until":          "2020-01-01T00:00:00Z",
			"strike_missing_thinking": true,
		},
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
	meta := guardMeta(saved)
	if meta.DisableKind != "" || meta.CooldownUntil != "" {
		t.Fatalf("probe success did not clear cooldown metadata: %#v", meta)
	}
	if meta.StrikeMissingThinking {
		t.Fatal("probe success did not clear the missing-thinking strike")
	}
}
func TestReconcileReleasesLegacyMarkerParseCooldown(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "cooldown_only" })
	verdict := QualityVerdict{Class: ClassExpectedMarkerMissing, Signals: Signals{
		Streaming:  true,
		ByteCount:  62000,
		EventCount: 1,
	}}
	if errApply := plugin.ApplyVerdict(context.Background(), "auth-1", verdict, "probe", ""); errApply != nil {
		t.Fatal(errApply)
	}
	before, _ := plugin.stateAccount("auth-1")
	if before.State != StateCooling || before.LastEvidence == nil {
		t.Fatalf("legacy cooldown was not established: %#v", before)
	}
	if errReconcile := plugin.Reconcile(context.Background()); errReconcile != nil {
		t.Fatal(errReconcile)
	}
	after, _ := plugin.stateAccount("auth-1")
	if after.State != StateSuspect || !after.CooldownUntil.IsZero() || after.DisableKind != "" {
		t.Fatalf("legacy cooldown was not released: %#v", after)
	}
	saved, errDecode := rawAuthToMap(host.auths["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if authDisabled(saved) || guardMeta(saved).DisableKind != "" || guardMeta(saved).CooldownUntil != "" {
		t.Fatalf("legacy cooldown metadata was not cleared: %#v", saved)
	}
	released := false
	for _, event := range plugin.store.Snapshot().Events {
		if event.AuthID == "auth-1" && event.Action == "probe" && event.Result == "released" {
			released = true
			break
		}
	}
	if !released {
		t.Fatal("legacy cooldown release was not recorded")
	}
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
	done := make(chan error, 1)
	go func() {
		done <- plugin.runDueProbes(context.Background())
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
		t.Fatal("runDueProbes did not finish")
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
	done := make(chan error, 1)
	go func() {
		done <- plugin.runDueProbes(context.Background())
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
		t.Fatal("runDueProbes did not finish")
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
