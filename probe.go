package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"

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
	accounts := p.catalogSnapshot()
	var wg sync.WaitGroup
	for _, account := range accounts {
		authID := account.ID
		wg.Add(1)
		go func() {
			defer wg.Done()
			if errProbe := p.ProbeAccount(ctx, authID, false, ""); errProbe != nil && (ctx == nil || ctx.Err() == nil) {
				_ = p.recordInternalError(authID, errProbe)
			}
		}()
	}
	wg.Wait()
	return nil
}

func (p *Plugin) probeDue(authID string, force bool) (bool, error) {
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
	if ctx == nil {
		ctx = context.Background()
	}
	if errCtx := ctx.Err(); errCtx != nil {
		return errCtx
	}
	due, errDue := p.probeDue(authID, force)
	if errDue != nil || !due {
		return errDue
	}
	// Reserve the account before taking a concurrency slot so a second probe for
	// the same account waits without occupying another slot
	if errBegin := p.beginAccountProbe(ctx, authID); errBegin != nil {
		return errBegin
	}
	defer p.endAccountProbe(authID)
	if errSlot := p.acquireProbeSlot(ctx); errSlot != nil {
		return errSlot
	}
	defer p.releaseProbeSlot()

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
	probeStartedAt := time.Now()
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
		return p.handleProbeFailure(authID, raw, response, reEnabled, QualityVerdict{Class: classifyTransportError(errStream), Reason: "probe transport error"}, errStream)
	}
	defer p.closeModelStream(stream.StreamID)
	if stream.StatusCode < 200 || stream.StatusCode >= 300 {
		return p.handleProbeFailure(authID, raw, response, reEnabled, QualityVerdict{Class: classifyProbeStatus(stream.StatusCode), Reason: fmt.Sprintf("upstream status %d", stream.StatusCode)}, fmt.Errorf("probe upstream status %d", stream.StatusCode))
	}
	analyzer := NewStreamAnalyzerWithProtocol(probeCfg.ExitProtocol)
	analyzer.SetEncryptedConfig(qualityCfg.MinEncryptedBytes, qualityCfg.EncryptedBytesPerReasoningToken)
	var bufferBytes int64
	var terminal bool
	for {
		select {
		case <-probeCtx.Done():
			return p.handleProbeFailure(authID, raw, response, reEnabled, QualityVerdict{Class: ClassProbeError, Reason: "probe context ended"}, probeCtx.Err())
		default:
		}
		read, errRead := p.readModelStreamContext(probeCtx, stream.StreamID)
		if errRead != nil {
			return p.handleProbeFailure(authID, raw, response, reEnabled, QualityVerdict{Class: classifyTransportError(errRead), Reason: "probe stream read error"}, errRead)
		}
		if len(read.Payload) > 0 {
			bufferBytes += int64(len(read.Payload))
			if bufferBytes > qualityCfg.HoldMaxBufferBytes {
				return p.handleProbeFailure(authID, raw, response, reEnabled, QualityVerdict{Class: ClassProbeError, Reason: "probe response exceeded buffer limit"}, fmt.Errorf("probe response exceeded buffer limit"))
			}
			analyzer.ObserveChunk(read.Payload)
		}
		if read.Error != "" {
			readErr := fmt.Errorf("%s", read.Error)
			return p.handleProbeFailure(authID, raw, response, reEnabled, QualityVerdict{Class: classifyTransportError(readErr), Reason: "probe stream error"}, readErr)
		}
		if read.Done {
			terminal = true
			break
		}
	}
	sig := analyzer.Finalize(terminal)
	sig.DurationMS = time.Since(probeStartedAt).Milliseconds()
	sig.GenerationMS = generationWindowMS(sig.DurationMS, sig.FirstTokenMS, sig.ReasoningTokens)
	sig.TPS = tps(sig.OutputTokens, sig.GenerationMS)
	markerOK := matchProbeMarker(sig.VisibleText, probeMarker)
	verdict := classifyProbeResult(sig, qualityCfg, markerOK, qualityCfg.ThinkingGuard)
	if verdict.Class == ClassNone {
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
	return p.handleProbeFailure(authID, raw, response, reEnabled, verdict, fmt.Errorf("probe quality check failed: %s", verdict.Reason))
}

func (p *Plugin) markProbeSuccess(authID string, raw map[string]json.RawMessage, response pluginapi.HostAuthGetResponse, sig Signals) error {
	meta := guardMeta(raw)
	if meta.Managed {
		meta.DisableKind = ""
		meta.CooldownUntil = ""
		meta.StrikeMissingThinking = false
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
	now := time.Now().UTC()
	verdict := QualityVerdict{Class: ClassNone, Reason: "probe passed", Signals: sig}
	return p.stateStore().Update(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		account.State = StateHealthy
		account.LastProbeAt = now
		account.LastObservedAt = now
		account.LastReason = ""
		account.LastAttempt = &AttemptEvidence{At: now, Source: "probe", Class: ClassNone, Reason: "probe passed"}
		account.LastEvidence = qualityEvidenceFromVerdict(verdict, "probe", now)
		account.CooldownUntil = time.Time{}
		account.ProbeBackoffUntil = time.Time{}
		account.DisableKind = ""
		account.QualityStrikeMissingThinking = false
		account.SoftStrikes = 0
		account.ErrorStrikes = 0
		account.ThinkingStrikes = 0
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

func (p *Plugin) handleProbeFailure(authID string, raw map[string]json.RawMessage, response pluginapi.HostAuthGetResponse, reEnabled bool, verdict QualityVerdict, errProbe error) error {
	// Keep marker and TPS verdicts out of internal error events
	if errProbe != nil && verdict.Class == ClassProbeError {
		_ = p.recordInternalError(authID, errProbe)
	}
	_, errApply := p.applyVerdictLocked(
		context.Background(),
		authID,
		verdict,
		"probe",
		"",
		raw,
		response,
		reEnabled,
	)
	return errApply
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

func matchProbeMarker(text, expected string) bool {
	expected = strings.TrimSpace(expected)
	if expected == "" {
		return true
	}
	text = strings.TrimRightFunc(text, unicode.IsSpace)
	lines := strings.Split(text, "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		line := strings.TrimSpace(lines[index])
		if line == "" {
			continue
		}
		return strings.EqualFold(line, expected)
	}
	return false
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

func (p *Plugin) probeConcurrency() int {
	if p == nil {
		return 1
	}
	p.mu.RLock()
	limit := p.policy.Probe.Concurrency
	p.mu.RUnlock()
	if limit <= 0 {
		return 1
	}
	return limit
}

func (p *Plugin) beginAccountProbe(ctx context.Context, authID string) error {
	p.probeGate.Lock()
	defer p.probeGate.Unlock()
	for {
		if _, busy := p.probing[authID]; !busy {
			if p.probing == nil {
				p.probing = make(map[string]struct{})
			}
			p.probing[authID] = struct{}{}
			return nil
		}
		if errWait := p.waitProbeLocked(ctx); errWait != nil {
			return errWait
		}
	}
}

func (p *Plugin) endAccountProbe(authID string) {
	if p == nil {
		return
	}
	p.probeGate.Lock()
	delete(p.probing, authID)
	p.probeCond.Broadcast()
	p.probeGate.Unlock()
}

func (p *Plugin) acquireProbeSlot(ctx context.Context) error {
	for {
		limit := p.probeConcurrency()
		p.probeGate.Lock()
		if p.probeActive < limit {
			if errCtx := ctx.Err(); errCtx != nil {
				p.probeGate.Unlock()
				return errCtx
			}
			p.probeActive++
			p.probeGate.Unlock()
			return nil
		}
		errWait := p.waitProbeLocked(ctx)
		p.probeGate.Unlock()
		if errWait != nil {
			return errWait
		}
	}
}

func (p *Plugin) releaseProbeSlot() {
	if p == nil {
		return
	}
	p.probeGate.Lock()
	if p.probeActive > 0 {
		p.probeActive--
	}
	p.probeCond.Broadcast()
	p.probeGate.Unlock()
}

func (p *Plugin) wakeProbeWaiters() {
	if p == nil || p.probeCond == nil {
		return
	}
	p.probeGate.Lock()
	p.probeCond.Broadcast()
	p.probeGate.Unlock()
}

// waitProbeLocked waits until a probe slot or account reservation changes.
// Caller must hold probeGate.
func (p *Plugin) waitProbeLocked(ctx context.Context) error {
	if ctx == nil {
		p.probeCond.Wait()
		return nil
	}
	if errCtx := ctx.Err(); errCtx != nil {
		return errCtx
	}
	stop := context.AfterFunc(ctx, func() {
		p.probeGate.Lock()
		p.probeCond.Broadcast()
		p.probeGate.Unlock()
	})
	p.probeCond.Wait()
	stop()
	return ctx.Err()
}
