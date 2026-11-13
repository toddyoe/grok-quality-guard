package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type verdictResult struct {
	Actioned     bool
	CrossVerify  bool
	ThresholdHit bool
}

// ApplyVerdict records one passive, transparent, or externally supplied
// observation. It serializes auth mutation with probes and Resin operations.
func (p *Plugin) ApplyVerdict(ctx context.Context, authID string, verdict QualityVerdict, source, callbackID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(authID) == "" {
		return nil
	}
	lock := p.accountLock(authID)
	lock.Lock()
	defer lock.Unlock()
	_, errApply := p.applyVerdictLocked(ctx, authID, verdict, source, callbackID, nil, pluginapi.HostAuthGetResponse{}, false)
	return errApply
}

// applyVerdictLocked is shared by passive observations and active probes.
// Callers must hold the per-auth lock.
func (p *Plugin) applyVerdictLocked(
	ctx context.Context,
	authID string,
	verdict QualityVerdict,
	source, _ string,
	raw map[string]json.RawMessage,
	response pluginapi.HostAuthGetResponse,
	reEnabled bool,
) (verdictResult, error) {
	if strings.TrimSpace(authID) == "" {
		return verdictResult{}, nil
	}
	store := p.stateStore()
	if store == nil {
		return verdictResult{}, errors.New("state store is not initialized")
	}
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()

	sig := verdict.Signals
	now := time.Now().UTC()
	previousState, _ := p.stateAccount(authID)
	priorMissingStrike := previousState.QualityStrikeMissingThinking
	backoff := nextProbeBackoff(policy.Probe, previousState)
	outcome := verdictResult{}

	errUpdate := store.Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		if priorMissingStrike {
			account.QualityStrikeMissingThinking = true
		}
		account.LastObservedAt = now
		account.LastAttempt = &AttemptEvidence{
			At: now, Source: source, Class: verdict.Class, Reason: verdict.Reason,
		}
		if verdict.Class != ClassIgnored && !isProbeErrorClass(verdict.Class) && !isAccountStatusClass(verdict.Class) {
			account.Metrics = Metrics{
				FirstTokenMS:    sig.FirstTokenMS,
				DurationMS:      sig.DurationMS,
				GenerationMS:    sig.GenerationMS,
				OutputTokens:    sig.OutputTokens,
				ReasoningTokens: sig.ReasoningTokens,
				VisibleTokens:   sig.VisibleTokens,
				EncryptedBytes:  sig.EncryptedBytes,
				TPS:             sig.TPS,
			}
			account.LastReason = verdict.Class
			account.LastEvidence = qualityEvidenceFromVerdict(verdict, source, now)
		}
		if source == "probe" {
			account.LastProbeAt = now
			if isProbeErrorClass(verdict.Class) || verdict.Class == ClassIgnored {
				account.ProbeBackoffUntil = now.Add(backoff)
			}
		}

		preserveIsolation := func() bool {
			return isIsolatedState(account.State)
		}
		eventAction := "observe"
		if source == "probe" {
			eventAction = "probe"
		}
		eventResult := "recorded"
		eventDetail := source
		if verdict.Reason != "" {
			eventDetail += " · " + verdict.Reason
		}
		eventClass := verdict.Class

		switch verdict.Class {
		case ClassNone:
			account.SoftStrikes = 0
			account.ErrorStrikes = 0
			account.ThinkingStrikes = 0
			account.QualityStrikeMissingThinking = false
			if !preserveIsolation() {
				account.State = StateHealthy
			}
			return nil

		case ClassSoftTPS, ClassInsufficientOutput, ClassInsufficientGeneration:
			account.ThinkingStrikes = 0
			account.SoftStrikes++
			threshold := positiveInt(policy.Quality.ConsecutiveSoft, 2)
			if account.SoftStrikes >= threshold {
				outcome.ThresholdHit = true
				if policy.Quality.SoftCrossVerify && source != "probe" {
					outcome.CrossVerify = true
					eventAction = "cross_verify_schedule"
					eventResult = "pending"
					eventDetail = fmt.Sprintf("%s · 连续软 TPS %d/%d，等待交叉验证", source, account.SoftStrikes, threshold)
					if !preserveIsolation() {
						account.State = StateSuspect
					}
				} else {
					outcome.Actioned = true
					eventResult = "threshold_reached"
					eventDetail = fmt.Sprintf("%s · 连续软 TPS %d/%d", source, account.SoftStrikes, threshold)
					if !preserveIsolation() {
						account.State = StateDegraded
					}
				}
			} else {
				eventResult = "pending"
				eventDetail = fmt.Sprintf("%s · 连续软 TPS %d/%d", source, account.SoftStrikes, threshold)
				if !preserveIsolation() {
					account.State = StateSuspect
				}
			}

		case ClassProbeError, ClassProxyError:
			account.ThinkingStrikes = 0
			account.ErrorStrikes++
			threshold := positiveInt(policy.Quality.ConsecutiveErrors, 2)
			if account.ErrorStrikes >= threshold {
				outcome.ThresholdHit = true
				outcome.Actioned = true
				eventResult = "threshold_reached"
				eventDetail = fmt.Sprintf("%s · 连续探测错误 %d/%d", source, account.ErrorStrikes, threshold)
				if verdict.Reason != "" {
					eventDetail += " · " + verdict.Reason
				}
				if !preserveIsolation() {
					account.State = StateDegraded
				}
			} else {
				eventResult = "pending"
				eventDetail = fmt.Sprintf("%s · 连续探测错误 %d/%d", source, account.ErrorStrikes, threshold)
				if verdict.Reason != "" {
					eventDetail += " · " + verdict.Reason
				}
				if !preserveIsolation() {
					account.State = StateSuspect
				}
			}

		case ClassCredentialInvalid, ClassQuotaExhausted:
			account.SoftStrikes = 0
			account.ErrorStrikes = 0
			account.ThinkingStrikes = 0
			outcome.Actioned = true
			eventResult = "actioned"

		case ClassIgnored:
			eventClass = ""
			eventResult = "ignored"
			eventDetail = source + " · 非质量类错误，未计入连续错误"
			if verdict.Reason != "" {
				eventDetail += " · " + verdict.Reason
			}

		case ClassMissingThinking:
			account.ThinkingStrikes++
			account.SoftStrikes = 0
			threshold := positiveInt(policy.Quality.ConsecutiveMissingThinking, 1)
			if account.ThinkingStrikes >= threshold {
				outcome.ThresholdHit = true
				if policy.Quality.ThinkingCrossVerify && source != "probe" {
					outcome.CrossVerify = true
					eventAction = "cross_verify_schedule"
					eventResult = "pending"
					eventDetail = fmt.Sprintf("%s · 连续缺少真实思考 %d/%d，等待交叉验证", source, account.ThinkingStrikes, threshold)
					if !preserveIsolation() {
						account.State = StateSuspect
					}
				} else {
					outcome.Actioned = true
					eventResult = "threshold_reached"
					eventDetail = fmt.Sprintf("%s · 连续缺少真实思考 %d/%d · permanent_eligible=%t", source, account.ThinkingStrikes, threshold, priorMissingStrike)
					if !preserveIsolation() {
						account.State = StateDegraded
					}
				}
			} else {
				eventResult = "pending"
				eventDetail = fmt.Sprintf("%s · 连续缺少真实思考 %d/%d", source, account.ThinkingStrikes, threshold)
				if !preserveIsolation() {
					account.State = StateSuspect
				}
			}

		default:
			if isConfirmedClass(verdict.Class) {
				account.ThinkingStrikes = 0
				outcome.ThresholdHit = true
				outcome.Actioned = true
				eventResult = "threshold_reached"
				if !preserveIsolation() {
					account.State = StateDegraded
				}
			} else if !preserveIsolation() {
				account.State = StateSuspect
			}
			eventDetail = source
			if verdict.Reason != "" {
				eventDetail += " · " + verdict.Reason
			}
		}

		p.recordEvent(state, authID, eventClass, eventAction, eventResult, eventDetail)
		return nil
	})
	if errUpdate != nil {
		return outcome, errUpdate
	}

	if verdict.Class == ClassNone {
		p.markRecentSuccess(authID)
	}

	if isAccountStatusClass(verdict.Class) {
		if raw == nil {
			_, resolvedResponse, resolvedRaw, errResolve := p.resolveAuth(ctx, authID)
			if errResolve != nil {
				return outcome, errResolve
			}
			response = resolvedResponse
			raw = resolvedRaw
		}
		_, errAction := p.applyAutoCooldownWithAuthLocked(ctx, authID, verdict, source, raw, response, false, true)
		return outcome, errAction
	}

	if outcome.CrossVerify {
		p.scheduleVerdictCrossVerify(authID, verdict.Class, source)
		return outcome, nil
	}
	if !outcome.Actioned {
		if source == "probe" && reEnabled {
			if errHold := p.holdProbeRecoveryLocked(ctx, authID, raw, response, verdict.Class, backoff); errHold != nil {
				return outcome, errHold
			}
		}
		return outcome, nil
	}
	if policy.AutoAction == "observe" {
		if source == "probe" && reEnabled {
			if errHold := p.holdProbeRecoveryLocked(ctx, authID, raw, response, verdict.Class, backoff); errHold != nil {
				return outcome, errHold
			}
		}
		return outcome, nil
	}
	permanentEligible := priorMissingStrike
	if raw == nil {
		_, resolvedResponse, resolvedRaw, errResolve := p.resolveAuth(ctx, authID)
		if errResolve != nil {
			return outcome, errResolve
		}
		raw = resolvedRaw
		response = resolvedResponse
	}
	if _, errAction := p.applyAutoCooldownWithAuthLocked(
		ctx,
		authID,
		verdict,
		source,
		raw,
		response,
		permanentEligible,
		true,
	); errAction != nil {
		return outcome, errAction
	}
	return outcome, nil
}

func isProbeErrorClass(class string) bool {
	return class == ClassProbeError || class == ClassProxyError
}

func positiveInt(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func (p *Plugin) scheduleVerdictCrossVerify(authID, class, source string) {
	if p == nil || strings.TrimSpace(authID) == "" {
		return
	}
	if p.hasActiveTask("quality.cross_verify", authID) {
		return
	}
	p.startTask("quality.cross_verify", []string{authID}, "正在交叉验证质量异常", func(ctx context.Context) (any, error) {
		if errProbe := p.ProbeAccount(ctx, authID, true, ""); errProbe != nil {
			return nil, errProbe
		}
		account, okAccount := p.findAccount(authID)
		if !okAccount {
			return nil, fmt.Errorf("auth not found")
		}
		return p.accountView(ctx, account)
	})
	_ = p.stateStore().Update(func(state *State) error {
		p.recordEvent(state, authID, class, "cross_verify_schedule", "scheduled", source)
		return nil
	})
}

func (p *Plugin) applyAutoCooldown(authID string, verdict QualityVerdict, source string) error {
	lock := p.accountLock(authID)
	lock.Lock()
	defer lock.Unlock()
	account, _ := p.stateAccount(authID)
	_, response, raw, errResolve := p.resolveAuth(context.Background(), authID)
	if errResolve != nil {
		return errResolve
	}
	_, errApply := p.applyAutoCooldownWithAuthLocked(
		context.Background(),
		authID,
		verdict,
		source,
		raw,
		response,
		account.QualityStrikeMissingThinking,
		false,
	)
	return errApply
}

func (p *Plugin) applyAutoCooldownWithAuthLocked(
	ctx context.Context,
	authID string,
	verdict QualityVerdict,
	source string,
	raw map[string]json.RawMessage,
	response pluginapi.HostAuthGetResponse,
	previousMissingStrike bool,
	force bool,
) (bool, error) {
	account, _ := p.stateAccount(authID)
	now := time.Now().UTC()
	if !force && isCooldownState(account.State) && now.Before(account.CooldownUntil) {
		return false, nil
	}
	if account.State == StateDisabledByGuard || account.State == StateExternallyDisabled || account.State == StateCredentialInvalid {
		return false, nil
	}
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	kind := DisableKindCooldown
	duration := cooldownForClass(verdict.Class, policy.Quality)
	switch verdict.Class {
	case ClassCredentialInvalid:
		kind = DisableKindCredentialInvalid
		duration = 0
	case ClassQuotaExhausted:
		kind = DisableKindQuota
		duration = quotaExhaustedCooldown
	default:
		if isProbeErrorClass(verdict.Class) {
			duration = nextProbeBackoff(policy.Probe, account)
		}
		if duration <= 0 {
			duration = policy.Quality.MissingThinkingCooldown.Duration
		}
	}
	allowPermanent := policy.AutoAction == "cooldown_then_disable"
	if allowPermanent && kind == DisableKindCooldown && verdict.Class == ClassMissingThinking && previousMissingStrike {
		kind = DisableKindPermanent
		duration = 0
	}
	strike := verdict.Class == ClassMissingThinking
	if errDisable := p.saveGuardDisableLocked(ctx, authID, response, raw, kind, duration, verdict.Class, source, strike); errDisable != nil {
		return false, errDisable
	}
	return true, nil
}

func (p *Plugin) holdProbeRecoveryLocked(
	ctx context.Context,
	authID string,
	raw map[string]json.RawMessage,
	response pluginapi.HostAuthGetResponse,
	class string,
	backoff time.Duration,
) error {
	if raw == nil {
		_, resolvedResponse, resolvedRaw, errResolve := p.resolveAuth(ctx, authID)
		if errResolve != nil {
			return errResolve
		}
		raw = resolvedRaw
		response = resolvedResponse
	}
	account, okAccount := p.stateAccount(authID)
	if !okAccount || account.DisableKind == "" || authDisabled(raw) {
		return nil
	}
	if account.DisableKind == DisableKindPermanent || account.DisableKind == DisableKindCredentialInvalid {
		return nil
	}
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	duration := backoff
	switch class {
	case ClassEmptyStream:
		duration = policy.Quality.EmptyStreamCooldown.Duration
	case ClassMissingThinking:
		duration = policy.Quality.MissingThinkingCooldown.Duration
	}
	if duration <= 0 {
		duration = policy.Quality.MissingThinkingCooldown.Duration
	}
	return p.saveGuardDisableLocked(ctx, authID, response, raw, DisableKindCooldown, duration, class, "probe", account.QualityStrikeMissingThinking)
}

func (p *Plugin) applyGuardDisable(authID, kind string, duration time.Duration, class, source string, strike bool) error {
	lock := p.accountLock(authID)
	lock.Lock()
	defer lock.Unlock()
	return p.applyGuardDisableLocked(context.Background(), authID, kind, duration, class, source, strike)
}

// applyGuardDisableLocked records the desired state before touching the auth
// file so an interrupted write can be repaired by the reconciler. Callers must
// hold the per-auth lock.
func (p *Plugin) applyGuardDisableLocked(ctx context.Context, authID, kind string, duration time.Duration, class, source string, strike bool) error {
	_, response, raw, errResolve := p.resolveAuth(ctx, authID)
	if errResolve != nil {
		return errResolve
	}
	return p.saveGuardDisableLocked(ctx, authID, response, raw, kind, duration, class, source, strike)
}

func (p *Plugin) saveGuardDisableLocked(
	_ context.Context,
	authID string,
	response pluginapi.HostAuthGetResponse,
	raw map[string]json.RawMessage,
	kind string,
	duration time.Duration,
	class, source string,
	strike bool,
) error {
	store := p.stateStore()
	if store == nil {
		return errors.New("state store is not initialized")
	}
	previousState, _ := p.stateAccount(authID)
	if authDisabled(raw) && previousState.DisableKind == "" {
		return p.setStateExternal(authID, class)
	}
	if kind == "" {
		kind = DisableKindCooldown
	}
	previousDisabled := previousState.PreviousDisabled
	if previousState.DisableKind == "" {
		previousDisabled = authDisabled(raw)
	}
	var until time.Time
	if kind == DisableKindCooldown || kind == DisableKindQuota {
		if duration <= 0 {
			p.mu.RLock()
			duration = p.policy.Quality.MissingThinkingCooldown.Duration
			p.mu.RUnlock()
		}
		until = time.Now().UTC().Add(duration)
	}
	errState := store.Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		account.State = stateForDisableKind(kind)
		account.CooldownUntil = until
		account.DisableKind = kind
		account.PreviousDisabled = previousDisabled
		account.QualityStrikeMissingThinking = account.QualityStrikeMissingThinking || strike
		if class != "" {
			account.LastReason = class
		}
		intent := true
		account.AuthDisabledIntent = &intent
		return nil
	})
	if errState != nil {
		return errState
	}
	if !authDisabled(raw) {
		if errSave := p.saveAuthDisabled(response.Name, raw, true); errSave != nil {
			return fmt.Errorf("save guard-disabled auth: %w", errSave)
		}
	}
	return store.Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		account.AuthDisabledIntent = nil
		p.recordEvent(state, authID, class, "guard_disable", kind, source)
		return nil
	})
}

func (p *Plugin) setStateExternal(authID, class string) error {
	return p.stateStore().Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		account.State = StateExternallyDisabled
		account.DisableKind = ""
		account.CooldownUntil = time.Time{}
		account.PreviousDisabled = false
		account.AuthDisabledIntent = nil
		if class != "" {
			account.LastReason = class
		}
		p.recordEvent(state, authID, class, "external_disable", "observed", "auth was disabled outside the guard")
		return nil
	})
}

func (p *Plugin) ManualCooldown(ctx context.Context, authID string, duration time.Duration) error {
	if duration <= 0 {
		p.mu.RLock()
		duration = p.policy.Quality.MissingThinkingCooldown.Duration
		p.mu.RUnlock()
	}
	return p.applyGuardDisable(authID, DisableKindCooldown, duration, "manual", "manual", false)
}

func (p *Plugin) ManualDisable(ctx context.Context, authID, reason string) error {
	if strings.TrimSpace(reason) == "" {
		reason = "manual"
	}
	return p.applyGuardDisable(authID, DisableKindPermanent, 0, reason, "manual", false)
}

func (p *Plugin) ManualRestore(ctx context.Context, authID string) error {
	lock := p.accountLock(authID)
	lock.Lock()
	defer lock.Unlock()
	_, response, raw, errResolve := p.resolveAuth(ctx, authID)
	if errResolve != nil {
		return errResolve
	}
	store := p.stateStore()
	if store == nil {
		return errors.New("state store is not initialized")
	}
	stateAccount, hasState := p.stateAccount(authID)
	guardOwned := hasState && stateAccount.DisableKind != ""
	if authDisabled(raw) && !guardOwned {
		if errUpdate := p.stateStore().Update(func(state *State) error {
			account := p.ensureStateAccount(state, authID, "", "")
			resetGuardDisableState(account)
			p.recordEvent(state, authID, "", "restore", "external_left_disabled", "manual")
			return nil
		}); errUpdate != nil {
			return errUpdate
		}
		return nil
	}
	if guardOwned {
		if errIntent := store.Update(func(state *State) error {
			account := p.ensureStateAccount(state, authID, "", "")
			intent := false
			account.AuthDisabledIntent = &intent
			return nil
		}); errIntent != nil {
			return errIntent
		}
	}
	if authDisabled(raw) {
		if errSave := p.saveAuthDisabled(response.Name, raw, false); errSave != nil {
			return fmt.Errorf("save restored auth: %w", errSave)
		}
	}
	return store.Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		resetGuardDisableState(account)
		p.recordEvent(state, authID, "", "restore", "restored", "manual")
		return nil
	})
}

func resetGuardDisableState(account *AccountState) {
	if account == nil {
		return
	}
	account.State = StateUnknown
	account.LastReason = ""
	account.CooldownUntil = time.Time{}
	account.ProbeBackoffUntil = time.Time{}
	account.DisableKind = ""
	account.PreviousDisabled = false
	account.AuthDisabledIntent = nil
	account.QualityStrikeMissingThinking = false
	account.SoftStrikes = 0
	account.ErrorStrikes = 0
	account.ThinkingStrikes = 0
}

func (p *Plugin) Reconcile(ctx context.Context) error {
	store := p.stateStore()
	if store == nil {
		return nil
	}
	snapshot := store.Snapshot()
	catalog := p.catalogSnapshot()
	accounts := make(map[string]Account, len(catalog))
	for _, account := range catalog {
		accounts[account.ID] = account
	}
	for authID, stateAccount := range snapshot.Accounts {
		if stateAccount == nil {
			continue
		}
		if stateAccount.AuthDisabledIntent != nil || isIsolatedState(stateAccount.State) {
			p.reconcileAccount(ctx, authID)
			continue
		}
		if stateAccount.State != StateUnknown {
			continue
		}
		account, okAccount := accounts[authID]
		if !okAccount || !account.Disabled {
			continue
		}
		p.reconcileAccount(ctx, authID)
	}
	return nil
}

func (p *Plugin) reconcileAccount(ctx context.Context, authID string) {
	lock := p.accountLock(authID)
	lock.Lock()
	defer lock.Unlock()

	_, response, raw, errResolve := p.resolveAuth(ctx, authID)
	if errResolve != nil {
		return
	}
	store := p.stateStore()
	if store == nil {
		return
	}
	stateAccount, okState := p.stateAccount(authID)
	if !okState {
		return
	}
	if stateAccount.AuthDisabledIntent != nil {
		target := *stateAccount.AuthDisabledIntent
		if target != authDisabled(raw) {
			if errSave := p.saveAuthDisabled(response.Name, raw, target); errSave != nil {
				return
			}
		}
		if target {
			_ = store.Update(func(state *State) error {
				account := p.ensureStateAccount(state, authID, "", "")
				account.AuthDisabledIntent = nil
				return nil
			})
			return
		}
		_ = store.Update(func(state *State) error {
			account := p.ensureStateAccount(state, authID, "", "")
			resetGuardDisableState(account)
			p.recordEvent(state, authID, "", "restore", "restored", "reconcile")
			return nil
		})
		return
	}
	if authDisabled(raw) {
		if stateAccount.DisableKind == "" {
			if stateAccount.State != StateExternallyDisabled {
				_ = p.setStateExternal(authID, "")
			}
			return
		}
		expectedState := stateForDisableKind(stateAccount.DisableKind)
		if stateAccount.State != expectedState {
			_ = p.updateStateAccount(authID, func(account *AccountState) error {
				account.State = expectedState
				return nil
			})
		}
		return
	}
	if stateAccount.DisableKind == "" {
		if stateAccount.State == StateExternallyDisabled {
			_ = p.updateStateAccount(authID, func(account *AccountState) error {
				account.State = StateUnknown
				return nil
			})
		}
		return
	}
	if isCooldownState(stateAccount.State) && time.Now().UTC().Before(stateAccount.CooldownUntil) {
		_ = p.saveAuthDisabled(response.Name, raw, true)
		return
	}
	_ = store.Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		resetGuardDisableState(account)
		p.recordEvent(state, authID, "", "restore", "restored", "reconcile")
		return nil
	})
}

func validateAuthPayload(raw map[string]json.RawMessage) error {
	if raw == nil {
		return errors.New("auth payload is empty")
	}
	if authRawString(raw, "access_token") == "" && authRawString(raw, "refresh_token") == "" {
		return errors.New("auth payload has no access_token or refresh_token")
	}
	return nil
}
