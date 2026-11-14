package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

const probePrompt = "先用三点总结为什么天空呈蓝色，最后一行只输出 QUALITY_OK。"
const probeMarker = "QUALITY_OK"
const probeReconcileInterval = 5 * time.Minute

func (p *Plugin) probeLoop(ctx context.Context) {
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	_ = p.Reconcile(ctx)
	scheduler := newProbeScheduler(p, ctx)
	defer scheduler.wait()
	scheduler.scanAndDispatch()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	reconcileTicker := time.NewTicker(probeReconcileInterval)
	defer reconcileTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scheduler.scanAndDispatch()
		case <-reconcileTicker.C:
			_ = p.Reconcile(ctx)
		case authID := <-scheduler.done:
			scheduler.finish(authID)
			scheduler.dispatch()
		}
	}
}

// probeScheduler bounds pending work while keeping active probe goroutines at
// the configured concurrency
type probeScheduler struct {
	plugin *Plugin
	ctx    context.Context
	done   chan string

	queue  []string
	queued map[string]struct{}
	active int
	wg     sync.WaitGroup
}

func newProbeScheduler(plugin *Plugin, ctx context.Context) *probeScheduler {
	if ctx == nil {
		ctx = context.Background()
	}
	return &probeScheduler{
		plugin: plugin,
		ctx:    ctx,
		done:   make(chan string),
		queued: make(map[string]struct{}),
	}
}

func (s *probeScheduler) scanAndDispatch() {
	if s == nil || s.plugin == nil {
		return
	}
	s.plugin.mu.RLock()
	policy := s.plugin.policy
	s.plugin.mu.RUnlock()
	if !policy.PassiveGuard && !policy.TransparentGuard {
		return
	}
	accounts := s.plugin.catalogSnapshot()
	store := s.plugin.stateStore()
	if store == nil {
		return
	}
	state := store.Snapshot()
	s.enqueueDue(accounts, state, policy, time.Now().UTC())
	s.dispatch()
}

func (s *probeScheduler) enqueueDue(accounts []Account, state State, policy Policy, now time.Time) int {
	if s == nil {
		return 0
	}
	limit := policy.Probe.QueueSize
	if limit <= 0 {
		limit = defaultProbeQueueSize
	}
	if limit > maxProbeQueueSize {
		limit = maxProbeQueueSize
	}
	added := 0
	for _, account := range accounts {
		if len(s.queue) >= limit {
			break
		}
		if _, exists := s.queued[account.ID]; exists {
			continue
		}
		stateAccount, okState := state.Accounts[account.ID]
		if !probeDueForAccount(stateAccount, okState, account.ID, policy, now) {
			continue
		}
		s.queued[account.ID] = struct{}{}
		s.queue = append(s.queue, account.ID)
		added++
	}
	return added
}

func (s *probeScheduler) dispatch() {
	if s == nil || s.plugin == nil {
		return
	}
	limit := s.plugin.probeConcurrency()
	for s.active < limit && len(s.queue) > 0 {
		authID := s.queue[0]
		s.queue = s.queue[1:]
		s.active++
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if errProbe := s.plugin.ProbeAccount(s.ctx, authID, false, ""); errProbe != nil && s.ctx.Err() == nil {
				_ = s.plugin.recordInternalError(authID, errProbe)
			}
			select {
			case s.done <- authID:
			case <-s.ctx.Done():
			}
		}()
	}
}

func (s *probeScheduler) finish(authID string) {
	if s == nil {
		return
	}
	if s.active > 0 {
		s.active--
	}
	delete(s.queued, authID)
}

func (s *probeScheduler) wait() {
	if s != nil {
		s.wg.Wait()
	}
}

func (s *probeScheduler) waitIdle() {
	if s == nil {
		return
	}
	for {
		select {
		case <-s.ctx.Done():
			s.wg.Wait()
			return
		default:
		}
		s.dispatch()
		if s.active == 0 && len(s.queue) == 0 {
			return
		}
		select {
		case authID := <-s.done:
			s.finish(authID)
		case <-s.ctx.Done():
			s.wg.Wait()
			return
		}
	}
}
func (p *Plugin) probeDue(authID string, force bool) (bool, error) {
	if force {
		return true, nil
	}
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	store := p.stateStore()
	if store == nil {
		return true, nil
	}
	state := store.Snapshot()
	account, okState := state.Accounts[authID]
	return probeDueForAccount(account, okState, authID, policy, time.Now().UTC()), nil
}

func probeDueForAccount(account *AccountState, okState bool, authID string, policy Policy, now time.Time) bool {
	if !okState || account == nil {
		return true
	}
	switch account.State {
	case StateHealthy, StateDisabledByGuard, StateExternallyDisabled, StateCredentialInvalid:
		return false
	case StateCooling, StateQuotaExhausted:
		if account.CooldownUntil.IsZero() {
			return true
		}
		return !now.Before(account.CooldownUntil)
	case StateSuspect:
		return account.ProbeBackoffUntil.IsZero() || !now.Before(account.ProbeBackoffUntil)
	case StateUnknown:
		if !account.ProbeBackoffUntil.IsZero() && now.Before(account.ProbeBackoffUntil) {
			return false
		}
		delay := policy.Probe.UnknownDelay.Duration
		if delay <= 0 {
			delay = 30 * time.Second
		}
		delay += unknownProbeJitter(authID, policy.Probe.UnknownJitter.Duration)
		if account.FirstSeenAt.IsZero() {
			return true
		}
		return !now.Before(account.FirstSeenAt.Add(delay))
	default:
		return false
	}
}

func unknownProbeJitter(authID string, max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(authID))
	return time.Duration(hash.Sum64() % uint64(max))
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
	if errSlot := p.acquireProbeSlot(ctx, force); errSlot != nil {
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
	stateAccount, okState := p.stateAccount(authID)
	wasDisabled := authDisabled(raw)
	if wasDisabled && (!okState || stateAccount.DisableKind == "") {
		return p.setStateExternal(authID, ClassProbeError)
	}
	if wasDisabled && (stateAccount.DisableKind == DisableKindPermanent || stateAccount.DisableKind == DisableKindCredentialInvalid) {
		return nil
	}
	if wasDisabled && !force && isCooldownState(stateAccount.State) {
		if !stateAccount.CooldownUntil.IsZero() && time.Now().UTC().Before(stateAccount.CooldownUntil) {
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
		class, reason := classifyTransportFailure(errStream)
		return p.handleProbeFailure(authID, raw, response, reEnabled, QualityVerdict{Class: class, Reason: reason}, errStream)
	}
	defer p.closeModelStream(stream.StreamID)
	if stream.StatusCode < 200 || stream.StatusCode >= 300 {
		errorBody := p.readModelStreamErrorBody(probeCtx, stream.StreamID)
		class, reason := classifyUpstreamStatus(stream.StatusCode, errorBody)
		return p.handleProbeFailure(authID, raw, response, reEnabled, QualityVerdict{Class: class, Reason: reason}, fmt.Errorf("probe %s", reason))
	}
	analyzer := NewStreamAnalyzerWithProtocolAt(probeCfg.ExitProtocol, probeStartedAt)
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
			class, reason := classifyTransportFailure(errRead)
			return p.handleProbeFailure(authID, raw, response, reEnabled, QualityVerdict{Class: class, Reason: reason}, errRead)
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
			class, reason := classifyTransportFailure(readErr)
			return p.handleProbeFailure(authID, raw, response, reEnabled, QualityVerdict{Class: class, Reason: reason}, readErr)
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
		if errSuccess := p.markProbeSuccess(authID, sig); errSuccess != nil {
			return errSuccess
		}
		return nil
	}
	class := verdict.Class
	if class == ClassNone {
		class = ClassProbeError
	}
	return p.handleProbeFailure(authID, raw, response, reEnabled, verdict, fmt.Errorf("probe quality check failed: %s", verdict.Reason))
}

func (p *Plugin) markProbeSuccess(authID string, sig Signals) error {
	now := time.Now().UTC()
	verdict := QualityVerdict{Class: ClassNone, Reason: "probe passed", Signals: sig}
	errUpdate := p.stateStore().Update(func(state *State) error {
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
		account.PreviousDisabled = false
		account.AuthDisabledIntent = nil
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
	if errUpdate == nil {
		p.refreshSelectionAccount(authID)
	}
	return errUpdate
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

func (p *Plugin) probeQueueSize() int {
	if p == nil {
		return defaultProbeQueueSize
	}
	p.mu.RLock()
	limit := p.policy.Probe.QueueSize
	p.mu.RUnlock()
	if limit <= 0 {
		return defaultProbeQueueSize
	}
	if limit > maxProbeQueueSize {
		return maxProbeQueueSize
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

func (p *Plugin) acquireProbeSlot(ctx context.Context, priority bool) error {
	if priority {
		p.probeGate.Lock()
		p.probePriorityWaiters++
		p.probeGate.Unlock()
		defer func() {
			p.probeGate.Lock()
			p.probePriorityWaiters--
			p.probeCond.Broadcast()
			p.probeGate.Unlock()
		}()
	}
	for {
		limit := p.probeConcurrency()
		p.probeGate.Lock()
		if p.probeActive < limit && (priority || p.probePriorityWaiters == 0) {
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
