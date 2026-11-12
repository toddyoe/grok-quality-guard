package main

import (
	"context"
	"strings"
	"testing"
	"time"

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
	host.scriptStream("auth-1", badStreamScript()...)
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
	visible := strings.Repeat("reliable systems need careful design. ", 10) + "QUALITY_OK"
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
