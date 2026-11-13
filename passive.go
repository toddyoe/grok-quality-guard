package main

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type rpcResponseInterceptRequest struct {
	pluginapi.ResponseInterceptRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcStreamChunkInterceptRequest struct {
	pluginapi.StreamChunkInterceptRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

func (p *Plugin) InterceptResponse(request []byte) []byte {
	raw, _ := okEnvelope(pluginapi.ResponseInterceptResponse{})
	var req rpcResponseInterceptRequest
	if len(request) == 0 || json.Unmarshal(request, &req) != nil {
		return raw
	}
	p.mu.RLock()
	prefixes := append([]string(nil), p.policy.ModelPrefixes...)
	passiveGuard := p.policy.PassiveGuard
	p.mu.RUnlock()
	if !passiveGuard || isProbeHeader(req.RequestHeaders) || !isGrokModel(firstNonEmpty(req.RequestedModel, req.Model), prefixes) {
		return raw
	}
	if req.RequestID == "" {
		return raw
	}
	analyzer := NewStreamAnalyzer()
	analyzer.SetStreaming(false)
	analyzer.ObserveJSON(req.Body)
	p.pendingMu.Lock()
	prunePendingLocked(p.pending, time.Now().UTC())
	p.pending[req.RequestID] = &PendingObservation{
		RequestID: req.RequestID,
		Body:      append([]byte(nil), req.RequestBody...),
		Analyzer:  analyzer,
		StartedAt: time.Now(),
		Completed: true,
		Source:    "response",
		Model:     firstNonEmpty(req.RequestedModel, req.Model),
	}
	p.pendingMu.Unlock()
	return raw
}

func (p *Plugin) InterceptStreamChunk(request []byte) []byte {
	raw, _ := okEnvelope(pluginapi.StreamChunkInterceptResponse{})
	var req rpcStreamChunkInterceptRequest
	if len(request) == 0 || json.Unmarshal(request, &req) != nil {
		return raw
	}
	p.mu.RLock()
	prefixes := append([]string(nil), p.policy.ModelPrefixes...)
	passiveGuard := p.policy.PassiveGuard
	p.mu.RUnlock()
	if !passiveGuard || isProbeHeader(req.RequestHeaders) || !isGrokModel(firstNonEmpty(req.RequestedModel, req.Model), prefixes) {
		return raw
	}
	if req.RequestID == "" {
		return raw
	}
	p.pendingMu.Lock()
	prunePendingLocked(p.pending, time.Now().UTC())
	pending := p.pending[req.RequestID]
	if req.ChunkIndex == pluginapi.StreamChunkHeaderInitIndex {
		analyzer := NewStreamAnalyzer()
		pending = &PendingObservation{
			RequestID: req.RequestID,
			Body:      append([]byte(nil), req.OriginalRequest...),
			Analyzer:  analyzer,
			StartedAt: time.Now(),
			Source:    "stream",
			Model:     firstNonEmpty(req.RequestedModel, req.Model),
		}
		if len(pending.Body) == 0 && len(req.RequestBody) > 0 {
			pending.Body = append([]byte(nil), req.RequestBody...)
		}
		p.pending[req.RequestID] = pending
		p.pendingMu.Unlock()
		return raw
	}
	if pending == nil {
		pending = &PendingObservation{
			RequestID: req.RequestID,
			Analyzer:  NewStreamAnalyzer(),
			StartedAt: time.Now(),
			Source:    "stream",
			Model:     firstNonEmpty(req.RequestedModel, req.Model),
		}
		p.pending[req.RequestID] = pending
	}
	pending.Analyzer.ObserveChunk(req.Body)
	p.pendingMu.Unlock()
	return raw
}

func (p *Plugin) HandleUsage(request []byte) {
	var record pluginapi.UsageRecord
	if len(request) == 0 || json.Unmarshal(request, &record) != nil {
		return
	}
	if record.AuthID == "" {
		return
	}
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	if !policy.PassiveGuard {
		return
	}
	if !isGrokModel(record.Model, policy.ModelPrefixes) && !isGrokProvider(record.Provider) {
		return
	}

	p.pendingMu.Lock()
	pending := p.pending[record.RequestID]
	delete(p.pending, record.RequestID)
	p.pendingMu.Unlock()
	// Transparent executions are audited by the executor before emission. Their
	// usage records have no passive observation and must not be judged twice.
	if pending == nil && policy.TransparentGuard {
		return
	}
	// Probe usage is internal and has no passive observation. Real requests keep
	// their pending observation and must still be recorded during a probe window.
	if pending == nil && p.isProbeAuth(record.AuthID) {
		return
	}
	if record.Failed {
		_ = p.ApplyVerdict(context.Background(), record.AuthID, QualityVerdict{Class: ClassIgnored, Reason: "unsuccessful"}, "passive", "")
		return
	}

	reasoningBody := []byte(nil)
	if pending != nil {
		reasoningBody = pending.Body
	}
	reasoningNeeded := usageReasoningRequired(record, reasoningBody)
	sig := Signals{
		Terminal:        true,
		Streaming:       record.Stream,
		FirstTokenMS:    record.TTFT.Milliseconds(),
		FirstTokenKnown: record.TTFT > 0,
		DurationMS:      record.Latency.Milliseconds(),
		OutputTokens:    record.Detail.OutputTokens,
		ReasoningTokens: record.Detail.ReasoningTokens,
		UsageReported:   true,
	}
	if pending != nil {
		sig = pending.Analyzer.Finalize(true)
		sig.Streaming = record.Stream
		sig.Terminal = true
		sig.UsageReported = true
		if record.TTFT > 0 {
			sig.FirstTokenMS = record.TTFT.Milliseconds()
			sig.FirstTokenKnown = true
		}
		if record.Latency > 0 {
			sig.DurationMS = record.Latency.Milliseconds()
		}
		sig.OutputTokens = record.Detail.OutputTokens
		sig.ReasoningTokens = maxInt64(0, record.Detail.ReasoningTokens)
		sig.GenerationMS = generationWindowMS(sig.DurationMS, sig.FirstTokenMS, sig.ReasoningTokens)
		sig.TPS = tps(sig.OutputTokens, sig.GenerationMS)
	}
	sig.ReasoningRequired = reasoningNeeded
	verdict := classifyPassiveAudit(sig, policy.Quality)

	if errApply := p.ApplyVerdict(context.Background(), record.AuthID, verdict, "passive", ""); errApply != nil {
		_ = p.recordInternalError(record.AuthID, errApply)
	}
}

func (p *Plugin) recordInternalError(authID string, errValue error) error {
	if p.stateStore() == nil {
		return errValue
	}
	_ = p.stateStore().Update(func(state *State) error {
		p.recordEvent(state, authID, "", "internal_error", "error", sanitizeError(errValue))
		return nil
	})
	return nil
}

func usageReasoningRequired(record pluginapi.UsageRecord, body []byte) bool {
	if len(body) > 0 && !reasoningRequired(body) {
		return false
	}
	if disabledReasoningValue(record.ReasoningEffort) {
		return false
	}
	for _, model := range []string{record.ResponseModel, record.Model, record.Alias} {
		if strings.TrimSpace(model) == "" {
			continue
		}
		if reasoningRequiredForModel(model, body) {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

var (
	redactUserInfoPattern   = regexp.MustCompile(`(?i)\b(https?|socks5h?)://[^/\s@]+@`)
	redactBearerPattern     = regexp.MustCompile(`(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]+`)
	redactHeaderPattern     = regexp.MustCompile(`(?i)\b(authorization|cookie|set-cookie)\s*:\s*[^\r\n]+`)
	redactJSONTokenPattern  = regexp.MustCompile(`(?i)("(?:access_token|refresh_token|id_token|admin_token|proxy_token|password|secret)"\s*:\s*")[^"]*(")`)
	redactQueryTokenPattern = regexp.MustCompile(`(?i)([?&](?:token|key|secret|password|api_key)=)[^&\s]+`)
)

func sanitizeError(errValue error) string {
	if errValue == nil {
		return ""
	}
	text := strings.TrimSpace(errValue.Error())
	text = redactUserInfoPattern.ReplaceAllString(text, "${1}://")
	text = redactBearerPattern.ReplaceAllString(text, "${1}[redacted]")
	text = redactHeaderPattern.ReplaceAllString(text, "${1}: [redacted]")
	text = redactJSONTokenPattern.ReplaceAllString(text, "${1}[redacted]${2}")
	text = redactQueryTokenPattern.ReplaceAllString(text, "${1}[redacted]")
	if len(text) > 512 {
		text = text[:512]
	}
	return text
}

func jsonError(code, message string) []byte {
	raw, _ := json.Marshal(map[string]any{"ok": false, "error": map[string]string{"code": code, "message": message}})
	return raw
}

func jsonOK(data any) []byte {
	raw, _ := json.Marshal(map[string]any{"ok": true, "data": data})
	return raw
}

const pendingObservationTTL = 10 * time.Minute
const pendingObservationLimit = 4096

func prunePendingLocked(pending map[string]*PendingObservation, now time.Time) {
	for requestID, observation := range pending {
		if observation == nil || now.Sub(observation.StartedAt) > pendingObservationTTL {
			delete(pending, requestID)
		}
	}
	for len(pending) >= pendingObservationLimit {
		oldestID := ""
		var oldest time.Time
		for requestID, observation := range pending {
			if observation == nil {
				delete(pending, requestID)
				continue
			}
			if oldestID == "" || observation.StartedAt.Before(oldest) {
				oldestID = requestID
				oldest = observation.StartedAt
			}
		}
		if oldestID == "" {
			return
		}
		delete(pending, oldestID)
	}
}
