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
	if raw != nil && guardMeta(raw).StrikeMissingThinking {
		priorMissingStrike = true
	}
	backoff := nextProbeBackoff(policy.Probe, previousState)
	outcome := verdictResult{}

	errUpdate := store.Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		if priorMissingStrike {
			account.QualityStrikeMissingThinking = true
		}
		if raw != nil && reEnabled {
			meta := guardMeta(raw)
			if meta.Managed {
				account.State = StateCooling
				account.DisableKind = meta.DisableKind
				if until, errUntil := time.Parse(time.RFC3339, strings.TrimSpace(meta.CooldownUntil)); errUntil == nil {
					account.CooldownUntil = until
				}
			}
		}
		account.LastObservedAt = now
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
		if source == "probe" {
			account.LastProbeAt = now
			if isProbeErrorClass(verdict.Class) || verdict.Class == ClassIgnored {
				account.ProbeBackoffUntil = now.Add(backoff)
			}
		}

		preserveIsolation := func() bool {
			switch account.State {
			case StateCooling, StateDisabledByGuard, StateExternallyDisabled:
				return true
			default:
				return false
			}
		}
		eventAction := "observe"
		eventResult := "recorded"
		eventDetail := source
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

		case ClassSoftTPS:
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
				if !preserveIsolation() {
					account.State = StateDegraded
				}
			} else {
				eventResult = "pending"
				eventDetail = fmt.Sprintf("%s · 连续探测错误 %d/%d", source, account.ErrorStrikes, threshold)
				if !preserveIsolation() {
					account.State = StateSuspect
				}
			}

		case ClassIgnored:
			eventClass = ""
			eventResult = "ignored"
			eventDetail = source + " · 非质量类错误，未计入连续错误"

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
	if !force && account.State == StateCooling && now.Before(account.CooldownUntil) {
		return false, nil
	}
	if account.State == StateDisabledByGuard || account.State == StateExternallyDisabled {
		return false, nil
	}
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	duration := cooldownForClass(verdict.Class, policy.Quality)
	if isProbeErrorClass(verdict.Class) {
		duration = nextProbeBackoff(policy.Probe, account)
	}
	if duration <= 0 {
		duration = policy.Quality.MissingThinkingCooldown.Duration
	}
	kind := DisableKindCooldown
	allowPermanent := policy.AutoAction == "cooldown_then_disable"
	if allowPermanent && verdict.Class == ClassMissingThinking && previousMissingStrike {
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
	meta := guardMeta(raw)
	if !meta.Managed || authDisabled(raw) {
		return nil
	}
	if meta.DisableKind == DisableKindPermanent {
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
	return p.saveGuardDisableLocked(ctx, authID, response, raw, DisableKindCooldown, duration, class, "probe", meta.StrikeMissingThinking)
}

func (p *Plugin) applyGuardDisable(authID, kind string, duration time.Duration, class, source string, strike bool) error {
	lock := p.accountLock(authID)
	lock.Lock()
	defer lock.Unlock()
	return p.applyGuardDisableLocked(context.Background(), authID, kind, duration, class, source, strike)
}

// applyGuardDisableLocked updates the auth file before the state store. Callers
// must hold the per-auth lock. A failed auth save leaves the previous state and
// binding intact.
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
	meta := guardMeta(raw)
	if authDisabled(raw) && !meta.Managed {
		return p.setStateExternal(authID, class)
	}
	previousDisabled := authDisabled(raw)
	if !meta.Managed {
		meta.Managed = true
		meta.PreviousDisabled = previousDisabled
	}
	if strike {
		meta.StrikeMissingThinking = true
	}
	meta.DisableKind = kind
	var until time.Time
	if kind == DisableKindCooldown {
		if duration <= 0 {
			p.mu.RLock()
			duration = p.policy.Quality.MissingThinkingCooldown.Duration
			p.mu.RUnlock()
		}
		until = time.Now().UTC().Add(duration)
		meta.CooldownUntil = until.Format(time.RFC3339)
	} else {
		meta.CooldownUntil = ""
	}
	setRawBool(raw, "disabled", true)
	if errMeta := setGuardMeta(raw, meta); errMeta != nil {
		return errMeta
	}
	encoded, errMarshal := marshalRawMap(raw)
	if errMarshal != nil {
		return errMarshal
	}
	if _, errSave := p.saveAuth(response.Name, encoded); errSave != nil {
		return fmt.Errorf("save guard-disabled auth: %w", errSave)
	}
	return p.stateStore().Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		if kind == DisableKindCooldown {
			account.State = StateCooling
			account.CooldownUntil = until
		} else {
			account.State = StateDisabledByGuard
			account.CooldownUntil = time.Time{}
		}
		if class != "" {
			account.LastReason = class
		}
		account.DisableKind = kind
		account.PreviousDisabled = meta.PreviousDisabled
		account.QualityStrikeMissingThinking = meta.StrikeMissingThinking
		p.recordEvent(state, authID, class, "guard_disable", kind, source)
		return nil
	})
}

func (p *Plugin) setStateExternal(authID, class string) error {
	return p.stateStore().Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		account.State = StateExternallyDisabled
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
	meta := guardMeta(raw)
	if authDisabled(raw) && !meta.Managed {
		if errUpdate := p.stateStore().Update(func(state *State) error {
			account := p.ensureStateAccount(state, authID, "", "")
			account.State = StateUnknown
			account.CooldownUntil = time.Time{}
			account.ProbeBackoffUntil = time.Time{}
			account.DisableKind = ""
			account.QualityStrikeMissingThinking = false
			account.SoftStrikes = 0
			account.ErrorStrikes = 0
			account.ThinkingStrikes = 0
			p.recordEvent(state, authID, "", "restore", "external_left_disabled", "manual")
			return nil
		}); errUpdate != nil {
			return errUpdate
		}
		return nil
	}
	if meta.Managed {
		setRawBool(raw, "disabled", false)
		meta.DisableKind = ""
		meta.CooldownUntil = ""
		meta.StrikeMissingThinking = false
		if errMeta := setGuardMeta(raw, meta); errMeta != nil {
			return errMeta
		}
		encoded, errMarshal := marshalRawMap(raw)
		if errMarshal != nil {
			return errMarshal
		}
		if _, errSave := p.saveAuth(response.Name, encoded); errSave != nil {
			return fmt.Errorf("save restored auth: %w", errSave)
		}
	}
	return p.stateStore().Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		account.State = StateUnknown
		account.LastReason = ""
		account.CooldownUntil = time.Time{}
		account.ProbeBackoffUntil = time.Time{}
		account.DisableKind = ""
		account.PreviousDisabled = false
		account.QualityStrikeMissingThinking = false
		account.SoftStrikes = 0
		account.ErrorStrikes = 0
		account.ThinkingStrikes = 0
		p.recordEvent(state, authID, "", "restore", "restored", "manual")
		return nil
	})
}

func (p *Plugin) Reconcile(ctx context.Context) error {
	store := p.stateStore()
	if store == nil {
		return nil
	}
	snapshot := store.Snapshot()
	for authID, stateAccount := range snapshot.Accounts {
		if stateAccount == nil {
			continue
		}
		if stateAccount.State != StateCooling && stateAccount.State != StateDisabledByGuard && stateAccount.State != StateUnknown && stateAccount.State != StateExternallyDisabled {
			continue
		}
		p.reconcileAccount(ctx, authID, stateAccount.State)
	}
	return nil
}

func (p *Plugin) reconcileAccount(ctx context.Context, authID, currentState string) {
	lock := p.accountLock(authID)
	lock.Lock()
	defer lock.Unlock()

	_, response, raw, errResolve := p.resolveAuth(ctx, authID)
	if errResolve != nil {
		return
	}
	meta := guardMeta(raw)
	if !authDisabled(raw) && !meta.Managed {
		_ = p.updateStateAccount(authID, func(account *AccountState) error {
			account.State = StateUnknown
			account.DisableKind = ""
			account.CooldownUntil = time.Time{}
			return nil
		})
		return
	}
	externalState := authDisabledState(raw)
	if externalState != "" && !meta.Managed {
		if currentState != StateExternallyDisabled {
			_ = p.setStateExternal(authID, "")
		}
		return
	}
	if !meta.Managed {
		return
	}
	switch meta.DisableKind {
	case DisableKindPermanent:
		_ = p.updateStateAccount(authID, func(account *AccountState) error {
			account.State = StateDisabledByGuard
			account.DisableKind = DisableKindPermanent
			return nil
		})
	case DisableKindCooldown:
		if !authDisabled(raw) {
			if errDisable := p.saveAuthDisabled(response.Name, raw, true); errDisable != nil {
				return
			}
		}
		until, _ := time.Parse(time.RFC3339, strings.TrimSpace(meta.CooldownUntil))
		_ = p.updateStateAccount(authID, func(account *AccountState) error {
			if account.State != StateDisabledByGuard {
				account.State = StateCooling
			}
			account.CooldownUntil = until
			account.DisableKind = DisableKindCooldown
			account.QualityStrikeMissingThinking = meta.StrikeMissingThinking
			return nil
		})
	}
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
