package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (p *Plugin) ApplyVerdict(ctx context.Context, authID string, verdict QualityVerdict, source, callbackID string) error {
	_ = ctx
	_ = callbackID
	if strings.TrimSpace(authID) == "" {
		return nil
	}
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()

	sig := verdict.Signals
	now := time.Now().UTC()
	if errUpdate := p.updateStateAccount(authID, func(account *AccountState) error {
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
		if verdict.Class != "" {
			account.LastReason = verdict.Class
		} else {
			account.LastReason = ""
		}
		switch verdict.Class {
		case ClassNone:
			if account.State != StateCooling && account.State != StateDisabledByGuard && account.State != StateExternallyDisabled {
				account.State = StateHealthy
			}
		case ClassSoftTPS, ClassProbeError, ClassProxyError:
			if account.State != StateCooling && account.State != StateDisabledByGuard && account.State != StateExternallyDisabled {
				account.State = StateSuspect
			}
		default:
			if isConfirmedClass(verdict.Class) {
				if account.State != StateCooling && account.State != StateDisabledByGuard && account.State != StateExternallyDisabled {
					account.State = StateDegraded
				}
			}
		}
		return nil
	}); errUpdate != nil {
		return errUpdate
	}

	if source == "probe" {
		return nil
	}
	if verdict.Class == ClassNone {
		p.markRecentSuccess(authID)
		return nil
	}
	if errEvent := p.stateStore().Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		p.recordEvent(state, authID, verdict.Class, "observe", "recorded", source)
		_ = account
		return nil
	}); errEvent != nil {
		return errEvent
	}
	if isConfirmedClass(verdict.Class) && policy.AutoAction == "cooldown_then_disable" {
		return p.applyAutoCooldown(authID, verdict, source)
	}
	return nil
}

func (p *Plugin) applyAutoCooldown(authID string, verdict QualityVerdict, source string) error {
	account, _ := p.stateAccount(authID)
	now := time.Now().UTC()
	if account.State == StateCooling && now.Before(account.CooldownUntil) {
		return nil
	}
	if account.State == StateDisabledByGuard || account.State == StateExternallyDisabled {
		return nil
	}
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	duration := cooldownForClass(verdict.Class, policy.Quality)
	if duration <= 0 {
		duration = policy.Quality.MissingThinkingCooldown.Duration
	}
	kind := DisableKindCooldown
	if verdict.Class == ClassMissingThinking && account.QualityStrikeMissingThinking {
		kind = DisableKindPermanent
		duration = 0
	}
	return p.applyGuardDisable(authID, kind, duration, verdict.Class, source, verdict.Class == ClassMissingThinking)
}

func (p *Plugin) applyGuardDisable(authID, kind string, duration time.Duration, class, source string, strike bool) error {
	lock := p.accountLock(authID)
	lock.Lock()
	defer lock.Unlock()
	return p.applyGuardDisableLocked(context.Background(), authID, kind, duration, class, source, strike)
}

// applyGuardDisableLocked updates the auth file before the state store. Callers
// must hold the per-auth lock. A failed auth save leaves the previous state and
// binding intact
func (p *Plugin) applyGuardDisableLocked(ctx context.Context, authID, kind string, duration time.Duration, class, source string, strike bool) error {
	_, response, raw, errResolve := p.resolveAuth(ctx, authID)
	if errResolve != nil {
		return errResolve
	}
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
