package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSuccessfulVerdictKeepsMissingThinkingStrike(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	if err := plugin.updateStateAccount("auth-1", func(account *AccountState) error {
		account.State = StateHealthy
		account.QualityStrikeMissingThinking = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := plugin.ApplyVerdict(context.Background(), "auth-1", QualityVerdict{Class: ClassNone, Signals: Signals{OutputTokens: 10}}, "passive", ""); err != nil {
		t.Fatal(err)
	}
	account, ok := plugin.stateAccount("auth-1")
	if !ok {
		t.Fatal("account state missing")
	}
	if !account.QualityStrikeMissingThinking {
		t.Fatal("successful request cleared the missing-thinking strike")
	}
}

func TestGuardDisablePreservesTokensAndWritesAuthFirst(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	if err := plugin.applyGuardDisable("auth-1", DisableKindCooldown, time.Hour, ClassMissingThinking, "test", true); err != nil {
		t.Fatal(err)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateCooling || !account.QualityStrikeMissingThinking {
		t.Fatalf("unexpected state after guard disable: %#v", account)
	}
	saved := host.saves["auth-1"]
	if len(saved) == 0 {
		t.Fatal("auth was not saved")
	}
	raw, err := rawAuthToMap(saved)
	if err != nil {
		t.Fatal(err)
	}
	if !authDisabled(raw) {
		t.Fatal("saved auth is not disabled")
	}
	if authRawString(raw, "access_token") != "access-auth-1" || authRawString(raw, "refresh_token") != "refresh-auth-1" {
		t.Fatal("token fields were not preserved")
	}
	meta := guardMeta(raw)
	if !meta.Managed || meta.DisableKind != DisableKindCooldown || !meta.StrikeMissingThinking || meta.CooldownUntil == "" {
		t.Fatalf("unexpected guard metadata: %#v", meta)
	}
}

func TestGuardDisableSaveFailureLeavesStateUntouched(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	host.saveErr = errors.New("disk full")
	plugin := newTestPlugin(t, host, nil)
	err := plugin.applyGuardDisable("auth-1", DisableKindCooldown, time.Hour, ClassMissingThinking, "test", true)
	if err == nil {
		t.Fatal("expected auth save failure")
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateUnknown || !account.CooldownUntil.IsZero() {
		t.Fatalf("state changed after failed auth save: %#v", account)
	}
	if len(host.saves) != 0 {
		t.Fatal("unexpected saved auth")
	}
}

func TestRecoveryProbeMissingThinkingAfterStrikeBecomesPermanent(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{
		"grok_quality_guard": map[string]any{
			"managed":                 true,
			"disable_kind":            DisableKindCooldown,
			"strike_missing_thinking": true,
		},
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	_, response, raw, errResolve := plugin.resolveAuth(context.Background(), "auth-1")
	if errResolve != nil {
		t.Fatal(errResolve)
	}
	lock := plugin.accountLock("auth-1")
	lock.Lock()
	errFailure := plugin.handleProbeFailure("auth-1", raw, response, true, ClassMissingThinking, nil)
	lock.Unlock()
	if errFailure != nil {
		t.Fatal(errFailure)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateDisabledByGuard || account.DisableKind != DisableKindPermanent {
		t.Fatalf("unexpected permanent disable state: %#v", account)
	}
	saved, err := rawAuthToMap(host.saves["auth-1"])
	if err != nil {
		t.Fatal(err)
	}
	meta := guardMeta(saved)
	if meta.DisableKind != DisableKindPermanent || !authDisabled(saved) {
		t.Fatalf("unexpected auth metadata: %#v", meta)
	}
}

func TestAutoCooldownDoesNotExtendActiveCooldown(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "cooldown_then_disable" })
	until := time.Now().UTC().Add(6 * time.Hour)
	if err := plugin.updateStateAccount("auth-1", func(account *AccountState) error {
		account.State = StateCooling
		account.CooldownUntil = until
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := plugin.applyAutoCooldown("auth-1", QualityVerdict{Class: ClassMissingThinking}, "test"); err != nil {
		t.Fatal(err)
	}
	account, _ := plugin.stateAccount("auth-1")
	if !account.CooldownUntil.Equal(until) {
		t.Fatalf("cooldown changed from %s to %s", until, account.CooldownUntil)
	}
	if len(host.saves) != 0 {
		t.Fatal("active cooldown unexpectedly wrote auth")
	}
}

func TestAutoCooldownAfterStrikeIsPermanent(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "cooldown_then_disable" })
	if err := plugin.updateStateAccount("auth-1", func(account *AccountState) error {
		account.State = StateHealthy
		account.QualityStrikeMissingThinking = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := plugin.applyAutoCooldown("auth-1", QualityVerdict{Class: ClassMissingThinking}, "test"); err != nil {
		t.Fatal(err)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateDisabledByGuard || account.DisableKind != DisableKindPermanent {
		t.Fatalf("unexpected state: %#v", account)
	}
}

func TestExternallyDisabledAuthIsNotModified(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{"disabled": true})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	if err := plugin.applyGuardDisable("auth-1", DisableKindCooldown, time.Hour, ClassMissingThinking, "test", true); err != nil {
		t.Fatal(err)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateExternallyDisabled {
		t.Fatalf("state = %q, want externally_disabled", account.State)
	}
	if len(host.saves) != 0 {
		t.Fatal("externally disabled auth was modified")
	}
}

func TestEmptyStreamUsesFifteenMinuteCooldown(t *testing.T) {
	cfg := DefaultQualityConfig()
	if got := cooldownForClass(ClassEmptyStream, cfg); got != 15*time.Minute {
		t.Fatalf("empty stream cooldown = %s", got)
	}
}
func TestRefreshCatalogMarksExternallyDisabled(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{"disabled": true})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateExternallyDisabled {
		t.Fatalf("state = %q, want externally_disabled", account.State)
	}
}

func TestReconcileRepairsTemporaryProbeEnable(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{
		"grok_quality_guard": map[string]any{
			"managed":        true,
			"disable_kind":   DisableKindCooldown,
			"cooldown_until": time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		},
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	if err := plugin.updateStateAccount("auth-1", func(account *AccountState) error {
		account.State = StateCooling
		account.CooldownUntil = time.Now().UTC().Add(time.Hour)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := plugin.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if !authDisabled(saved) {
		t.Fatal("reconcile did not restore the guard-owned disabled flag")
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateCooling || account.DisableKind != DisableKindCooldown {
		t.Fatalf("unexpected reconciled state: %#v", account)
	}
}

func TestCooldownOnlyRecoveryFailureKeepsCooling(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"grok_quality_guard": map[string]any{
			"managed":                 true,
			"disable_kind":            DisableKindCooldown,
			"strike_missing_thinking": true,
		},
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "cooldown_only" })
	_, response, raw, errResolve := plugin.resolveAuth(context.Background(), "auth-1")
	if errResolve != nil {
		t.Fatal(errResolve)
	}
	lock := plugin.accountLock("auth-1")
	lock.Lock()
	errFailure := plugin.handleProbeFailure("auth-1", raw, response, true, ClassMissingThinking, nil)
	lock.Unlock()
	if errFailure != nil {
		t.Fatal(errFailure)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateCooling || account.DisableKind != DisableKindCooldown {
		t.Fatalf("cooldown-only mode did not keep cooling: %#v", account)
	}
	if account.CooldownUntil.IsZero() {
		t.Fatal("cooldown deadline was not refreshed")
	}
	saved, err := rawAuthToMap(host.saves["auth-1"])
	if err != nil {
		t.Fatal(err)
	}
	if !authDisabled(saved) || guardMeta(saved).DisableKind != DisableKindCooldown {
		t.Fatalf("unexpected auth metadata: %#v", guardMeta(saved))
	}
}

func TestCooldownOnlyAppliesCooldownOnConfirmedAnomaly(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "cooldown_only" })
	if err := plugin.ApplyVerdict(context.Background(), "auth-1", QualityVerdict{Class: ClassMissingThinking}, "passive", ""); err != nil {
		t.Fatal(err)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateCooling || account.DisableKind != DisableKindCooldown {
		t.Fatalf("cooldown-only anomaly was not cooled: %#v", account)
	}
	if account.CooldownUntil.IsZero() {
		t.Fatal("cooldown deadline is missing")
	}
}
