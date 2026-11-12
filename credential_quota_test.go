package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestClassifyUpstreamStatusCredentialAndQuota(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, want: ClassCredentialInvalid},
		{name: "proxy auth", status: http.StatusProxyAuthRequired, want: ClassProxyError},
		{name: "payment required", status: http.StatusPaymentRequired, want: ClassQuotaExhausted},
		{name: "free quota exhausted", status: http.StatusTooManyRequests, body: `{"code":"subscription:free-usage-exhausted","error":"tokens (actual/limit): 10/10"}`, want: ClassQuotaExhausted},
		{name: "model quota exhausted", status: http.StatusForbidden, body: `{"error":"You've used all the included free usage for model grok-4.7"}`, want: ClassQuotaExhausted},
		{name: "generic rate limit", status: http.StatusTooManyRequests, body: `{"error":"rate limit exceeded; retry later"}`, want: ClassIgnored},
		{name: "forbidden credential", status: http.StatusForbidden, body: `{"error":"invalid token"}`, want: ClassCredentialInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, reason := classifyUpstreamStatus(test.status, []byte(test.body))
			if got != test.want {
				t.Fatalf("class = %q, want %q (reason=%q)", got, test.want, reason)
			}
			if reason == "" {
				t.Fatal("classification reason is empty")
			}
		})
	}
}

func TestCredentialInvalidDisablesAccountEvenInObserveMode(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "user@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "observe" })

	errApply := plugin.ApplyVerdict(context.Background(), "auth-1", QualityVerdict{
		Class: ClassCredentialInvalid, Reason: "upstream status 401 (credential rejected)",
	}, "probe", "")
	if errApply != nil {
		t.Fatal(errApply)
	}
	account, okAccount := plugin.stateAccount("auth-1")
	if !okAccount || account.State != StateCredentialInvalid || account.DisableKind != DisableKindCredentialInvalid {
		t.Fatalf("account state = %#v", account)
	}
	if !account.CooldownUntil.IsZero() {
		t.Fatalf("credential-invalid state must not have a cooldown: %#v", account)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if !authDisabled(saved) || guardMeta(saved).DisableKind != DisableKindCredentialInvalid {
		t.Fatalf("auth was not disabled with credential-invalid metadata: %#v", saved)
	}
	if state := authDisabledState(saved); state != StateCredentialInvalid {
		t.Fatalf("authDisabledState = %q, want %q", state, StateCredentialInvalid)
	}
}

func TestQuotaExhaustedCoolsFor24HoursEvenInObserveMode(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "user@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "observe" })

	before := time.Now().UTC()
	errApply := plugin.ApplyVerdict(context.Background(), "auth-1", QualityVerdict{
		Class: ClassQuotaExhausted, Reason: "upstream status 429 (quota exhausted)",
	}, "probe", "")
	if errApply != nil {
		t.Fatal(errApply)
	}
	account, okAccount := plugin.stateAccount("auth-1")
	if !okAccount || account.State != StateQuotaExhausted || account.DisableKind != DisableKindQuota {
		t.Fatalf("account state = %#v", account)
	}
	minimum := before.Add(quotaExhaustedCooldown - 2*time.Second)
	maximum := before.Add(quotaExhaustedCooldown + 2*time.Second)
	if account.CooldownUntil.Before(minimum) || account.CooldownUntil.After(maximum) {
		t.Fatalf("cooldown = %s, want approximately %s", account.CooldownUntil, before.Add(quotaExhaustedCooldown))
	}
	if due, _ := plugin.probeDue("auth-1", false); due {
		t.Fatal("quota-exhausted account must not be probed before cooldown expires")
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	meta := guardMeta(saved)
	if !authDisabled(saved) || meta.DisableKind != DisableKindQuota || meta.CooldownUntil == "" {
		t.Fatalf("auth was not disabled with quota metadata: %#v", saved)
	}
	if state := authDisabledState(saved); state != StateQuotaExhausted {
		t.Fatalf("authDisabledState = %q, want %q", state, StateQuotaExhausted)
	}
}
func TestProbeUnauthorizedMarksCredentialInvalid(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "user@example.com", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStreamStatus("auth-1", http.StatusUnauthorized,
		streamData(`{"error":"invalid token"}`),
		pluginapi.HostModelStreamReadResponse{Done: true},
	)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "observe" })

	if errProbe := plugin.ProbeAccount(context.Background(), "auth-1", true, ""); errProbe != nil {
		t.Fatal(errProbe)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateCredentialInvalid || account.DisableKind != DisableKindCredentialInvalid {
		t.Fatalf("account state = %#v", account)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if !authDisabled(saved) || guardMeta(saved).DisableKind != DisableKindCredentialInvalid {
		t.Fatalf("probe did not disable credential-invalid auth: %#v", saved)
	}
}

func TestProbeQuota429MarksQuotaExhausted(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "user@example.com", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStreamStatus("auth-1", http.StatusTooManyRequests,
		streamData(`{"code":"subscription:free-usage-exhausted","error":"tokens (actual/limit): 622160/600000"}`),
		pluginapi.HostModelStreamReadResponse{Done: true},
	)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "observe" })

	before := time.Now().UTC()
	if errProbe := plugin.ProbeAccount(context.Background(), "auth-1", true, ""); errProbe != nil {
		t.Fatal(errProbe)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateQuotaExhausted || account.DisableKind != DisableKindQuota {
		t.Fatalf("account state = %#v", account)
	}
	if !account.CooldownUntil.After(before.Add(quotaExhaustedCooldown - 2*time.Second)) {
		t.Fatalf("quota cooldown was not applied: %#v", account)
	}
	saved, errDecode := rawAuthToMap(host.saves["auth-1"])
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	meta := guardMeta(saved)
	if !authDisabled(saved) || meta.DisableKind != DisableKindQuota || meta.CooldownUntil == "" {
		t.Fatalf("probe did not cool quota-exhausted auth: %#v", saved)
	}
}
func TestProbeUnauthorizedWithoutStreamIDMarksCredentialInvalid(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "user@example.com", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStreamStatusWithoutID("auth-1", http.StatusUnauthorized)
	plugin := newTestPlugin(t, host, func(cfg *Config) { cfg.AutoAction = "observe" })

	if errProbe := plugin.ProbeAccount(context.Background(), "auth-1", true, ""); errProbe != nil {
		t.Fatal(errProbe)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateCredentialInvalid || account.DisableKind != DisableKindCredentialInvalid {
		t.Fatalf("account state = %#v", account)
	}
}
