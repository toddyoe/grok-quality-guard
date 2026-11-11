package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

const probePrompt = "Write exactly 16 numbered lines about reliable distributed systems. Each line must be one complete English sentence, with no markdown heading. The final line must end with the exact marker QUALITY_OK."
const probeMarker = "QUALITY_OK"

func (p *Plugin) probeLoop(ctx context.Context) {
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	_ = p.Reconcile(ctx)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = p.Reconcile(ctx)
			_ = p.runDueProbes(ctx)
		}
	}
}

func (p *Plugin) runDueProbes(ctx context.Context) error {
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	if !policy.PassiveGuard && !policy.TransparentGuard {
		return nil
	}
	for _, account := range p.catalogSnapshot() {
		if errProbe := p.ProbeAccount(ctx, account.ID, false, ""); errProbe != nil {
			_ = p.recordInternalError(account.ID, errProbe)
		}
	}
	return nil
}

func (p *Plugin) probeDue(ctx context.Context, authID string, force bool) (bool, error) {
	if force {
		return true, nil
	}
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	stateAccount, okState := p.stateAccount(authID)
	if !okState {
		return true, nil
	}
	now := time.Now().UTC()
	switch stateAccount.State {
	case StateHealthy, StateDisabledByGuard, StateExternallyDisabled:
		return false, nil
	case StateCooling:
		if stateAccount.CooldownUntil.IsZero() {
			return true, nil
		}
		return !now.Before(stateAccount.CooldownUntil), nil
	case StateSuspect:
		return stateAccount.ProbeBackoffUntil.IsZero() || !now.Before(stateAccount.ProbeBackoffUntil), nil
	case StateUnknown:
		delay := policy.Probe.UnknownDelay.Duration
		if stateAccount.FirstSeenAt.IsZero() {
			return true, nil
		}
		return now.Sub(stateAccount.FirstSeenAt) >= delay, nil
	default:
		return false, nil
	}
}

func (p *Plugin) ProbeAccount(ctx context.Context, authID string, force bool, callbackID string) error {
	if strings.TrimSpace(authID) == "" {
		return fmt.Errorf("auth_id is required")
	}
	due, errDue := p.probeDue(ctx, authID, force)
	if errDue != nil || !due {
		return errDue
	}
	p.probeMu.Lock()
	defer p.probeMu.Unlock()

	lock := p.accountLock(authID)
	lock.Lock()
	defer lock.Unlock()

	account, response, raw, errResolve := p.resolveAuth(ctx, authID)
	if errResolve != nil {
		return errResolve
	}
	if !isGrokAuth(raw) {
		return nil
	}
	meta := guardMeta(raw)
	wasDisabled := authDisabled(raw)
	if wasDisabled && !meta.Managed {
		return p.setStateExternal(authID, ClassProbeError)
	}
	if wasDisabled && meta.DisableKind == DisableKindPermanent {
		return nil
	}
	if wasDisabled && !force && meta.DisableKind == DisableKindCooldown {
		until, _ := time.Parse(time.RFC3339, strings.TrimSpace(meta.CooldownUntil))
		if !until.IsZero() && time.Now().UTC().Before(until) {
			return nil
		}
	}
	reEnabled := false
	if wasDisabled {
		if errEnable := p.saveAuthDisabled(response.Name, raw, false); errEnable != nil {
			return errEnable
		}
		reEnabled = true
	}
	p.markProbeAuth(account.ID, 3*time.Minute)
	p.mu.RLock()
	probeCfg := p.policy.Probe
	qualityCfg := p.policy.Quality
	p.mu.RUnlock()
	timeout := probeCfg.Timeout.Duration
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	body := probeBody(probeCfg)
	stream, errStream := p.executeModelStream(probeCtx, pluginapi.HostModelExecutionRequest{
		EntryProtocol:  probeCfg.EntryProtocol,
		ExitProtocol:   probeCfg.ExitProtocol,
		Model:          probeCfg.Model,
		Stream:         true,
		Body:           body,
		Headers:        http.Header{"Content-Type": []string{"application/json"}, probeHeader: []string{"1"}},
		ForcedProvider: "xai",
		AuthID:         account.ID,
		ProxyURL:       strings.TrimSpace(authRawString(raw, "proxy_url")),
	}, callbackID)
	if errStream != nil {
		return p.handleProbeFailure(authID, raw, response, reEnabled, classifyTransportError(errStream), errStream)
	}
	defer p.closeModelStream(stream.StreamID)
	if stream.StatusCode < 200 || stream.StatusCode >= 300 {
		return p.handleProbeFailure(authID, raw, response, reEnabled, ClassProbeError, fmt.Errorf("probe upstream status %d", stream.StatusCode))
	}
	analyzer := NewStreamAnalyzer()
	var bufferBytes int64
	var terminal bool
	for {
		select {
		case <-probeCtx.Done():
			return p.handleProbeFailure(authID, raw, response, reEnabled, ClassProbeError, probeCtx.Err())
		default:
		}
		read, errRead := p.readModelStreamContext(probeCtx, stream.StreamID)
		if errRead != nil {
			return p.handleProbeFailure(authID, raw, response, reEnabled, classifyTransportError(errRead), errRead)
		}
		if len(read.Payload) > 0 {
			bufferBytes += int64(len(read.Payload))
			if bufferBytes > qualityCfg.HoldMaxBufferBytes {
				return p.handleProbeFailure(authID, raw, response, reEnabled, ClassProbeError, fmt.Errorf("probe response exceeded buffer limit"))
			}
			analyzer.ObserveChunk(read.Payload)
		}
		if read.Error != "" {
			readErr := fmt.Errorf("%s", read.Error)
			return p.handleProbeFailure(authID, raw, response, reEnabled, classifyTransportError(readErr), readErr)
		}
		if read.Done {
			terminal = true
			break
		}
	}
	sig := analyzer.Finalize(terminal)
	sig.FirstTokenMS = 0
	verdict := classifyFinal(sig, qualityCfg, true, qualityCfg.MinPassiveOutputTokens, true)
	markerOK := strings.Contains(sig.VisibleText, probeMarker)
	if verdict.Class == ClassNone && markerOK && sig.VisibleTokens >= qualityCfg.MinPassiveOutputTokens {
		if errSuccess := p.markProbeSuccess(authID, raw, response, sig); errSuccess != nil {
			return errSuccess
		}
		p.markRecentSuccess(authID)
		return nil
	}
	class := verdict.Class
	if class == ClassNone {
		class = ClassProbeError
	}
	return p.handleProbeFailure(authID, raw, response, reEnabled, class, fmt.Errorf("probe did not satisfy quality marker"))
}

func (p *Plugin) markProbeSuccess(authID string, raw map[string]json.RawMessage, response pluginapi.HostAuthGetResponse, sig Signals) error {
	meta := guardMeta(raw)
	if meta.Managed {
		meta.DisableKind = ""
		meta.CooldownUntil = ""
		if errMeta := setGuardMeta(raw, meta); errMeta != nil {
			return errMeta
		}
		setRawBool(raw, "disabled", false)
		encoded, errMarshal := marshalRawMap(raw)
		if errMarshal != nil {
			return errMarshal
		}
		if _, errSave := p.saveAuth(response.Name, encoded); errSave != nil {
			return errSave
		}
	}
	return p.stateStore().Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		account.State = StateHealthy
		account.LastProbeAt = time.Now().UTC()
		account.LastReason = ""
		account.CooldownUntil = time.Time{}
		account.ProbeBackoffUntil = time.Time{}
		account.DisableKind = ""
		account.QualityStrikeMissingThinking = meta.StrikeMissingThinking
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
		p.recordEvent(state, authID, "", "probe", "healthy", "probe passed")
		return nil
	})
}

func (p *Plugin) handleProbeFailure(authID string, raw map[string]json.RawMessage, response pluginapi.HostAuthGetResponse, reEnabled bool, class string, errProbe error) error {
	_ = response
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	if errProbe != nil {
		_ = p.recordInternalError(authID, errProbe)
	}
	previousState, _ := p.stateAccount(authID)
	backoff := nextProbeBackoff(policy.Probe, previousState)
	now := time.Now().UTC()
	meta := guardMeta(raw)
	if reEnabled || (meta.Managed && meta.DisableKind != "") {
		kind := DisableKindCooldown
		duration := cooldownForClass(class, policy.Quality)
		switch class {
		case ClassMissingThinking:
			if meta.StrikeMissingThinking {
				kind = DisableKindPermanent
				duration = 0
			}
		case ClassEmptyStream:
			duration = policy.Quality.EmptyStreamCooldown.Duration
		case ClassProbeError, ClassProxyError:
			duration = backoff
		}
		if duration <= 0 && kind == DisableKindCooldown {
			duration = policy.Quality.MissingThinkingCooldown.Duration
		}
		if errDisable := p.applyGuardDisableLocked(context.Background(), authID, kind, duration, class, "probe", class == ClassMissingThinking); errDisable != nil {
			return errDisable
		}
	}
	return p.stateStore().Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		switch account.State {
		case StateCooling, StateDisabledByGuard, StateExternallyDisabled:
		default:
			account.State = StateSuspect
		}
		account.LastProbeAt = now
		account.ProbeBackoffUntil = now.Add(backoff)
		if class != "" {
			account.LastReason = class
		}
		p.recordEvent(state, authID, class, "probe", "failed", sanitizeError(errProbe))
		return nil
	})
}
func (p *Plugin) saveAuthDisabled(name string, raw map[string]json.RawMessage, disabled bool) error {
	setRawBool(raw, "disabled", disabled)
	encoded, errMarshal := marshalRawMap(raw)
	if errMarshal != nil {
		return errMarshal
	}
	_, errSave := p.saveAuth(name, encoded)
	return errSave
}

func probeBody(cfg ProbeConfig) []byte {
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		model = "grok-4.5"
	}
	body := map[string]any{
		"model":             model,
		"stream":            true,
		"max_output_tokens": cfg.MaxOutputTokens,
		"input": []any{map[string]any{
			"type": "message",
			"role": "user",
			"content": []any{map[string]any{
				"type": "input_text",
				"text": probePrompt,
			}},
		}},
	}
	raw, _ := json.Marshal(body)
	return raw
}
func nextProbeBackoff(cfg ProbeConfig, previous AccountState) time.Duration {
	base := cfg.SuspectBackoff.Duration
	if base <= 0 {
		base = 15 * time.Minute
	}
	maxBackoff := cfg.MaxBackoff.Duration
	if maxBackoff <= 0 {
		maxBackoff = time.Hour
	}
	if maxBackoff < base {
		maxBackoff = base
	}
	backoff := base
	if !previous.LastProbeAt.IsZero() && !previous.ProbeBackoffUntil.IsZero() {
		previousWindow := previous.ProbeBackoffUntil.Sub(previous.LastProbeAt)
		if previousWindow > 0 {
			backoff = previousWindow * 2
		}
	}
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	return backoff
}
