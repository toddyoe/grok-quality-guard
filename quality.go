package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	ClassBufferedBurst      = "buffered_burst"
	ClassCipherDrool        = "cipher_drool"
	ClassFakeEncryptedDump  = "fake_encrypted_dump"
	ClassFastReasoningRatio = "fast_reasoning_ratio_dump"
	ClassMissingThinking    = "missing_thinking"
	ClassHardTPS            = "hard_tps"
	ClassSoftTPS            = "soft_tps"
	ClassEmptyStream        = "empty_stream"
	ClassProbeError         = "probe_error"
	ClassProxyError         = "proxy_error"
	ClassIgnored            = "ignored"
	ClassNone               = ""
)

type Signals struct {
	HasThinking       bool
	HasReasoningDelta bool
	ReasoningStarted  bool
	VisibleTokens     int64
	VisibleRunes      int64
	ReasoningTokens   int64
	OutputTokens      int64
	EncryptedBytes    int64
	EncryptedFloor    int64
	UsageReported     bool
	FirstVisible      bool
	VisibleFlushMS    int64
	Terminal          bool
	HoldExpired       bool
	SemanticOutput    bool
	Streaming         bool
	FirstTokenMS      int64
	DurationMS        int64
	GenerationMS      int64
	TPS               float64
	VisibleText       string
}

type QualityVerdict struct {
	Class   string
	Reason  string
	Signals Signals
}

type SSEEvent struct {
	Event   string
	Data    string
	Comment string
}

type SSEParser struct {
	buffer  []byte
	event   string
	data    []string
	comment string
}

func (p *SSEParser) Feed(chunk []byte) []SSEEvent {
	if p == nil || len(chunk) == 0 {
		return nil
	}
	p.buffer = append(p.buffer, chunk...)
	var events []SSEEvent
	for {
		idx := bytes.IndexByte(p.buffer, '\n')
		if idx < 0 {
			break
		}
		line := p.buffer[:idx]
		p.buffer = p.buffer[idx+1:]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if ev, ok := p.processLine(string(line)); ok {
			events = append(events, ev)
		}
	}
	return events
}

func (p *SSEParser) Flush() []SSEEvent {
	if p == nil {
		return nil
	}
	var events []SSEEvent
	if len(p.buffer) > 0 {
		line := strings.TrimSuffix(string(p.buffer), "\r")
		p.buffer = nil
		if ev, ok := p.processLine(line); ok {
			events = append(events, ev)
		}
	}
	if ev, ok := p.dispatch(); ok {
		events = append(events, ev)
	}
	return events
}

func (p *SSEParser) processLine(line string) (SSEEvent, bool) {
	if line == "" {
		return p.dispatch()
	}
	if strings.HasPrefix(line, ":") {
		p.comment = strings.TrimSpace(strings.TrimPrefix(line, ":"))
		return SSEEvent{}, false
	}
	field, value, ok := strings.Cut(line, ":")
	if !ok {
		field = line
		value = ""
	}
	value = strings.TrimPrefix(value, " ")
	switch field {
	case "event":
		p.event = value
	case "data":
		p.data = append(p.data, value)
	}
	return SSEEvent{}, false
}

func (p *SSEParser) dispatch() (SSEEvent, bool) {
	if p.event == "" && len(p.data) == 0 && p.comment == "" {
		return SSEEvent{}, false
	}
	ev := SSEEvent{
		Event:   p.event,
		Data:    strings.Join(p.data, "\n"),
		Comment: p.comment,
	}
	p.event = ""
	p.data = nil
	p.comment = ""
	return ev, true
}

type StreamAnalyzer struct {
	parser         SSEParser
	streaming      bool
	signals        Signals
	startedAt      time.Time
	firstChunkAt   time.Time
	firstVisibleAt time.Time
	lastVisibleAt  time.Time
	terminalAt     time.Time
	visibleText    strings.Builder
	visibleDelta   bool
	reasoningDelta bool
	seenEncrypted  map[string]struct{}
}

func NewStreamAnalyzer() *StreamAnalyzer {
	return &StreamAnalyzer{
		startedAt:     time.Now(),
		streaming:     true,
		seenEncrypted: make(map[string]struct{}),
	}
}

func (a *StreamAnalyzer) SetStreaming(value bool) {
	if a == nil {
		return
	}
	a.streaming = value
	a.signals.Streaming = value
}

// Preview returns an immutable analysis snapshot without flushing a partial SSE
// line before terminal delivery
func (a *StreamAnalyzer) Preview() Signals {
	if a == nil {
		return Signals{}
	}
	sig := a.signals
	sig.Streaming = a.streaming
	sig.HasThinking = a.reasoningDelta || sig.HasReasoningDelta || a.encryptedMeetsFloor()
	sig.EncryptedFloor = encryptedFloor(sig.ReasoningTokens, QualityConfig{})
	now := time.Now()
	if a.terminalAt.IsZero() {
		sig.VisibleFlushMS = durationMS(a.firstVisibleAt, now)
	} else {
		sig.VisibleFlushMS = durationMS(a.firstVisibleAt, a.terminalAt)
	}
	if !a.firstVisibleAt.IsZero() && a.terminalAt.IsZero() {
		sig.VisibleFlushMS = durationMS(a.firstVisibleAt, now)
	}
	sig.FirstTokenMS = durationMS(a.startedAt, a.firstChunkAt)
	sig.DurationMS = durationMS(a.startedAt, now)
	sig.GenerationMS = generationWindowMS(sig.DurationMS, sig.FirstTokenMS, sig.ReasoningTokens)
	sig.TPS = tps(sig.OutputTokens, sig.GenerationMS)
	return sig
}

func (a *StreamAnalyzer) ObserveChunk(chunk []byte) {
	if a == nil || len(chunk) == 0 {
		return
	}
	for _, ev := range a.parser.Feed(chunk) {
		a.observeEvent(ev, time.Now())
	}
}

func (a *StreamAnalyzer) ObserveJSON(raw []byte) {
	if a == nil || len(raw) == 0 {
		return
	}
	a.observeJSON(raw, time.Now())
}

func (a *StreamAnalyzer) observeEvent(ev SSEEvent, now time.Time) {
	if strings.TrimSpace(ev.Comment) != "" && strings.Contains(strings.ToLower(ev.Comment), "grok2api-reasoning-start") {
		a.signals.ReasoningStarted = true
		a.markChunk(now)
	}
	data := strings.TrimSpace(ev.Data)
	if data == "" {
		return
	}
	if data == "[DONE]" {
		a.signals.Terminal = true
		a.terminalAt = now
		return
	}
	a.markChunk(now)
	a.observeJSON([]byte(data), now)
	if strings.EqualFold(ev.Event, "response.completed") || strings.EqualFold(ev.Event, "message_stop") {
		a.signals.Terminal = true
		a.terminalAt = now
	}
}

func (a *StreamAnalyzer) markChunk(now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}
	if a.firstChunkAt.IsZero() {
		a.firstChunkAt = now
	}
	if a.startedAt.IsZero() {
		a.startedAt = now
	}
}

func (a *StreamAnalyzer) observeJSON(raw []byte, now time.Time) {
	var payload map[string]any
	if errUnmarshal := json.Unmarshal(raw, &payload); errUnmarshal != nil {
		return
	}
	eventName := strings.ToLower(strings.TrimSpace(asString(payload["type"])))
	if eventName == "" {
		eventName = strings.ToLower(strings.TrimSpace(asString(payload["event"])))
	}
	a.observeUsage(payload)

	if choices, ok := payload["choices"].([]any); ok {
		for _, rawChoice := range choices {
			choice, _ := rawChoice.(map[string]any)
			if choice == nil {
				continue
			}
			if finish := strings.TrimSpace(asString(choice["finish_reason"])); finish != "" {
				a.signals.Terminal = true
				a.terminalAt = now
			}
			if delta, okDelta := choice["delta"].(map[string]any); okDelta {
				a.observeChatDelta(delta, now)
			}
			if message, okMessage := choice["message"].(map[string]any); okMessage && !a.visibleDelta {
				a.observeChatMessage(message, now)
			}
		}
	}

	switch eventName {
	case "response.output_item.added", "response.output_item.done":
		a.observeResponseItem(payload, now, eventName == "response.output_item.done")
	case "response.reasoning_text.delta", "response.reasoning_summary_text.delta":
		a.addReasoningText(asString(payload["delta"]))
	case "response.output_text.delta":
		a.addVisibleText(asString(payload["delta"]), now)
	case "response.output_text.done":
		if !a.visibleDelta {
			a.addVisibleText(asString(payload["text"]), now)
		}
	case "response.completed", "response.done":
		a.signals.Terminal = true
		a.terminalAt = now
	case "content_block_start":
		a.observeAnthropicBlockStart(payload, now)
	case "content_block_delta":
		a.observeAnthropicDelta(payload, now)
	case "content_block_stop":
		// Nothing to do; message_stop is the terminal marker
	case "message_delta":
		a.observeUsage(payload)
	case "message_stop":
		a.signals.Terminal = true
		a.terminalAt = now
	}

	if eventName == "response.completed" {
		if response, ok := payload["response"].(map[string]any); ok {
			a.observeUsage(response)
		}
	}
}

func (a *StreamAnalyzer) observeChatDelta(delta map[string]any, now time.Time) {
	if text := firstString(delta, "reasoning", "reasoning_content", "thinking_content"); strings.TrimSpace(text) != "" {
		a.addReasoningText(text)
	}
	a.addContentParts(delta["content"], now)
	if _, ok := delta["tool_calls"]; ok {
		a.signals.SemanticOutput = true
	}
	if _, ok := delta["function_call"]; ok {
		a.signals.SemanticOutput = true
	}
}

func (a *StreamAnalyzer) observeChatMessage(message map[string]any, now time.Time) {
	if text := firstString(message, "reasoning", "reasoning_content", "thinking_content"); strings.TrimSpace(text) != "" {
		a.addReasoningText(text)
	}
	a.addContentParts(message["content"], now)
	if _, ok := message["tool_calls"]; ok {
		a.signals.SemanticOutput = true
	}
}

func (a *StreamAnalyzer) observeResponseItem(payload map[string]any, now time.Time, done bool) {
	item, _ := payload["item"].(map[string]any)
	if item == nil {
		return
	}
	itemType := strings.ToLower(strings.TrimSpace(asString(item["type"])))
	switch itemType {
	case "reasoning":
		if !done {
			a.signals.ReasoningStarted = true
		}
		if enc := asString(item["encrypted_content"]); strings.TrimSpace(enc) != "" {
			a.addEncrypted(enc)
		}
		if summaries, ok := item["summary"].([]any); ok {
			for _, rawSummary := range summaries {
				summary, _ := rawSummary.(map[string]any)
				if summary != nil {
					a.addReasoningText(asString(summary["text"]))
				}
			}
		}
		if content, ok := item["content"].([]any); ok {
			for _, rawPart := range content {
				part, _ := rawPart.(map[string]any)
				if part != nil {
					a.addReasoningText(asString(part["text"]))
				}
			}
		}
	case "message":
		if !a.visibleDelta {
			a.addContentParts(item["content"], now)
		}
	case "function_call", "custom_tool_call", "tool_call", "local_shell_call", "apply_patch_call":
		a.signals.SemanticOutput = true
	}
}

func (a *StreamAnalyzer) observeAnthropicBlockStart(payload map[string]any, now time.Time) {
	block, _ := payload["content_block"].(map[string]any)
	if block == nil {
		return
	}
	switch strings.ToLower(asString(block["type"])) {
	case "thinking", "redacted_thinking":
		if text := asString(block["thinking"]); strings.TrimSpace(text) != "" {
			a.addReasoningText(text)
		}
		if data := asString(block["data"]); strings.TrimSpace(data) != "" {
			a.addEncrypted(data)
		}
		if text := asString(block["text"]); strings.TrimSpace(text) != "" {
			a.addReasoningText(text)
		}
		a.signals.ReasoningStarted = true
	case "text":
		a.addVisibleText(asString(block["text"]), now)
	case "tool_use":
		a.signals.SemanticOutput = true
	}
}

func (a *StreamAnalyzer) observeAnthropicDelta(payload map[string]any, now time.Time) {
	delta, _ := payload["delta"].(map[string]any)
	if delta == nil {
		return
	}
	switch strings.ToLower(asString(delta["type"])) {
	case "thinking_delta":
		a.addReasoningText(firstString(delta, "thinking", "text"))
	case "signature_delta":
		a.addEncrypted(asString(delta["signature"]))
	case "text_delta":
		a.addVisibleText(asString(delta["text"]), now)
	case "input_json_delta":
		a.signals.SemanticOutput = true
	}
}

func (a *StreamAnalyzer) addContentParts(value any, now time.Time) {
	switch typed := value.(type) {
	case string:
		a.addVisibleText(typed, now)
	case []any:
		for _, rawPart := range typed {
			part, _ := rawPart.(map[string]any)
			if part == nil {
				continue
			}
			kind := strings.ToLower(asString(part["type"]))
			if kind == "output_text" || kind == "text" {
				a.addVisibleText(asString(part["text"]), now)
			}
			if kind == "reasoning_text" || kind == "summary_text" {
				a.addReasoningText(asString(part["text"]))
			}
			if kind == "tool_call" || kind == "function_call" {
				a.signals.SemanticOutput = true
			}
		}
	}
}

func (a *StreamAnalyzer) addReasoningText(text string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	a.signals.HasReasoningDelta = true
	a.reasoningDelta = true
}

func (a *StreamAnalyzer) addVisibleText(text string, now time.Time) {
	if text == "" {
		return
	}
	a.visibleDelta = true
	if a.firstVisibleAt.IsZero() {
		a.firstVisibleAt = now
		a.signals.FirstVisible = true
	}
	a.lastVisibleAt = now
	a.visibleText.WriteString(text)
	if a.visibleText.Len() > 1024*1024 {
		current := a.visibleText.String()
		a.visibleText.Reset()
		a.visibleText.WriteString(current[len(current)-512*1024:])
	}
	a.signals.VisibleRunes += int64(utf8.RuneCountInString(text))
	a.signals.VisibleTokens = int64(math.Ceil(float64(a.signals.VisibleRunes) / 4.0))
}

func (a *StreamAnalyzer) addEncrypted(value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	if a.seenEncrypted == nil {
		a.seenEncrypted = make(map[string]struct{})
	}
	if _, ok := a.seenEncrypted[value]; ok {
		return
	}
	a.seenEncrypted[value] = struct{}{}
	a.signals.EncryptedBytes += int64(len([]byte(value)))
}

func (a *StreamAnalyzer) observeUsage(payload map[string]any) {
	if payload == nil {
		return
	}
	usage, _ := payload["usage"].(map[string]any)
	if usage == nil {
		if response, ok := payload["response"].(map[string]any); ok {
			usage, _ = response["usage"].(map[string]any)
		}
	}
	if usage == nil {
		return
	}
	a.signals.UsageReported = true
	if value := firstInt(usage, "completion_tokens", "output_tokens"); value > 0 {
		a.signals.OutputTokens = value
	}
	if value := firstInt(usage, "reasoning_tokens", "thinking_tokens"); value >= 0 {
		if value > 0 || usage["reasoning_tokens"] != nil || usage["thinking_tokens"] != nil {
			a.signals.ReasoningTokens = value
		}
	}
	for _, key := range []string{"completion_tokens_details", "output_tokens_details"} {
		if detail, ok := usage[key].(map[string]any); ok {
			if value := firstInt(detail, "reasoning_tokens", "thinking_tokens"); value >= 0 {
				a.signals.ReasoningTokens = value
			}
		}
	}
}

func (a *StreamAnalyzer) Finalize(ended bool) Signals {
	if a == nil {
		return Signals{}
	}
	for _, ev := range a.parser.Flush() {
		a.observeEvent(ev, time.Now())
	}
	if ended {
		a.signals.Terminal = true
		if a.terminalAt.IsZero() {
			a.terminalAt = time.Now()
		}
	}
	now := time.Now()
	if a.terminalAt.IsZero() {
		a.terminalAt = now
	}
	a.signals.Streaming = a.streaming
	a.signals.HasThinking = a.reasoningDelta || a.signals.HasReasoningDelta || a.encryptedMeetsFloor()
	if a.signals.OutputTokens <= 0 {
		a.signals.OutputTokens = a.signals.VisibleTokens + a.signals.ReasoningTokens
	}
	a.signals.EncryptedFloor = encryptedFloor(a.signals.ReasoningTokens, QualityConfig{})
	if a.signals.EncryptedFloor < 0 {
		a.signals.EncryptedFloor = 0
	}
	if a.signals.EncryptedBytes > 0 && a.signals.EncryptedBytes >= a.signals.EncryptedFloor {
		a.signals.HasThinking = true
	}
	if a.firstVisibleAt.IsZero() && a.signals.FirstVisible {
		a.firstVisibleAt = a.firstChunkAt
	}
	if !a.firstVisibleAt.IsZero() {
		end := a.terminalAt
		if end.IsZero() {
			end = a.lastVisibleAt
		}
		a.signals.VisibleFlushMS = end.Sub(a.firstVisibleAt).Milliseconds()
		if a.signals.VisibleFlushMS < 0 {
			a.signals.VisibleFlushMS = 0
		}
	}
	a.signals.FirstTokenMS = durationMS(a.startedAt, a.firstChunkAt)
	a.signals.DurationMS = durationMS(a.startedAt, a.terminalAt)
	a.signals.GenerationMS = a.signals.DurationMS - a.signals.FirstTokenMS
	a.signals.TPS = tps(a.signals.OutputTokens, a.signals.GenerationMS)
	a.signals.VisibleText = a.visibleText.String()
	return a.signals
}

func (a *StreamAnalyzer) encryptedMeetsFloor() bool {
	if a.signals.EncryptedBytes <= 0 {
		return false
	}
	floor := encryptedFloor(a.signals.ReasoningTokens, QualityConfig{})
	return a.signals.EncryptedBytes >= floor
}

func encryptedFloor(reasoningTokens int64, cfg QualityConfig) int64 {
	if cfg.MinEncryptedBytes <= 0 {
		cfg.MinEncryptedBytes = 256
	}
	if cfg.EncryptedBytesPerReasoningToken <= 0 {
		cfg.EncryptedBytesPerReasoningToken = 4
	}
	floor := cfg.MinEncryptedBytes
	if reasoningTokens > 0 {
		if reasoningTokens > math.MaxInt64/cfg.EncryptedBytesPerReasoningToken {
			return math.MaxInt64
		}
		need := reasoningTokens * cfg.EncryptedBytesPerReasoningToken
		if need > floor {
			floor = need
		}
	}
	return floor
}

func durationMS(start, end time.Time) int64 {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return 0
	}
	return end.Sub(start).Milliseconds()
}

func tps(outputTokens, generationMS int64) float64 {
	if outputTokens <= 0 || generationMS <= 0 {
		return 0
	}
	return float64(outputTokens) * 1000 / float64(generationMS)
}

func generationWindowMS(durationMS, firstTokenMS, reasoningTokens int64) int64 {
	window := durationMS - firstTokenMS
	if reasoningTokens > 0 && window < firstTokenMS && window < 1000 {
		return durationMS
	}
	return window
}

func classifyFinal(sig Signals, cfg QualityConfig, reasoningRequired bool, minOutput int64, streaming bool) QualityVerdict {
	sig.EncryptedFloor = encryptedFloor(sig.ReasoningTokens, cfg)
	sig.HasThinking = sig.HasReasoningDelta || (sig.EncryptedBytes > 0 && sig.EncryptedBytes >= sig.EncryptedFloor)
	if sig.OutputTokens <= 0 {
		sig.OutputTokens = sig.VisibleTokens + sig.ReasoningTokens
	}
	sig.GenerationMS = generationWindowMS(sig.DurationMS, sig.FirstTokenMS, sig.ReasoningTokens)
	sig.TPS = tps(sig.OutputTokens, sig.GenerationMS)
	sig.Streaming = streaming

	if streaming && qualityIsBurstDump(sig, cfg) {
		return QualityVerdict{Class: ClassBufferedBurst, Reason: "short generation window dumped output", Signals: sig}
	}
	if streaming && qualityIsCipherDrool(sig, minOutput) {
		return QualityVerdict{Class: ClassCipherDrool, Reason: "cipher-only stream is dumping visible text", Signals: sig}
	}
	if streaming && qualityIsFakeEncryptedDump(sig) {
		return QualityVerdict{Class: ClassFakeEncryptedDump, Reason: "encrypted or billed reasoning preceded a fast visible dump", Signals: sig}
	}
	if streaming && qualityIsFastReasoningRatioDump(sig) {
		return QualityVerdict{Class: ClassFastReasoningRatio, Reason: "reasoning tokens dominate a fast visible dump", Signals: sig}
	}
	if isEmptyStream(sig) {
		return QualityVerdict{Class: ClassEmptyStream, Reason: "upstream stream completed without content", Signals: sig}
	}
	if reasoningRequired && !sig.HasThinking && !isSemanticOnly(sig) {
		return QualityVerdict{Class: ClassMissingThinking, Reason: "no real reasoning evidence", Signals: sig}
	}
	if sig.Terminal && sig.OutputTokens >= minOutput && sig.GenerationMS > 0 {
		if sig.TPS >= cfg.HardTPS {
			return QualityVerdict{Class: ClassHardTPS, Reason: "output tokens per second exceeded hard threshold", Signals: sig}
		}
		if sig.TPS >= cfg.SoftTPS {
			return QualityVerdict{Class: ClassSoftTPS, Reason: "output tokens per second exceeded soft threshold", Signals: sig}
		}
	}
	return QualityVerdict{Class: ClassNone, Signals: sig}
}

func qualityIsBurstDump(sig Signals, cfg QualityConfig) bool {
	if sig.HasReasoningDelta {
		return false
	}
	heavy := sig.ReasoningTokens >= 80 || (sig.EncryptedBytes > 0 && sig.EncryptedBytes >= sig.EncryptedFloor)
	if !heavy {
		return false
	}
	if sig.HoldExpired && sig.VisibleTokens > 0 && sig.VisibleTokens < 32 {
		return true
	}
	if sig.FirstVisible && sig.VisibleFlushMS >= 0 && sig.VisibleFlushMS < 1000 {
		return true
	}
	return sig.GenerationMS > 0 && sig.GenerationMS < cfg.MinGenerationMS && sig.TPS >= cfg.SoftTPS
}

func qualityIsCipherDrool(sig Signals, minOutput int64) bool {
	if sig.HasReasoningDelta || sig.ReasoningTokens > 0 || sig.EncryptedBytes <= 0 || (sig.EncryptedFloor > 0 && sig.EncryptedBytes < sig.EncryptedFloor) {
		return false
	}
	if minOutput <= 0 {
		minOutput = 8
	}
	return sig.VisibleTokens >= 1024 || (sig.Terminal && sig.VisibleTokens >= minOutput)
}

func qualityIsFakeEncryptedDump(sig Signals) bool {
	if sig.HasReasoningDelta || (sig.EncryptedBytes <= 0 && sig.ReasoningTokens < 80) {
		return false
	}
	if !sig.FirstVisible || sig.VisibleFlushMS >= 2000 {
		return false
	}
	return sig.ReasoningTokens >= 80 || (sig.EncryptedBytes > 0 && sig.EncryptedBytes >= sig.EncryptedFloor)
}

func qualityIsFastReasoningRatioDump(sig Signals) bool {
	if !sig.HasReasoningDelta || !sig.FirstVisible || sig.VisibleFlushMS >= 2000 {
		return false
	}
	output := sig.OutputTokens
	if output <= 0 {
		output = sig.VisibleTokens + sig.ReasoningTokens
	}
	if output <= 0 || sig.ReasoningTokens <= 0 {
		return false
	}
	return sig.ReasoningTokens*5 >= output*4
}

func isEmptyStream(sig Signals) bool {
	return sig.Terminal && !sig.HasThinking && sig.ReasoningTokens <= 0 && sig.OutputTokens <= 0 && sig.VisibleTokens <= 0 && !sig.SemanticOutput
}

func isSemanticOnly(sig Signals) bool {
	return sig.SemanticOutput && !sig.HasThinking && sig.VisibleTokens == 0
}

func reasoningRequired(body []byte) bool {
	if len(body) == 0 {
		return true
	}
	var payload map[string]any
	if errUnmarshal := json.Unmarshal(body, &payload); errUnmarshal != nil {
		return true
	}
	if effort := strings.ToLower(strings.TrimSpace(asString(payload["reasoning_effort"]))); effort == "none" || effort == "off" {
		return false
	}
	for _, key := range []string{"reasoning", "output_config", "thinking"} {
		if raw, ok := payload[key]; ok {
			if disabledReasoningValue(raw) {
				return false
			}
		}
	}
	return true
}

func disabledReasoningValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return !typed
	case string:
		normalized := strings.ToLower(strings.TrimSpace(typed))
		return normalized == "none" || normalized == "off" || normalized == "disabled" || normalized == "false"
	case map[string]any:
		if effort := strings.ToLower(strings.TrimSpace(asString(typed["effort"]))); effort == "none" || effort == "off" {
			return true
		}
		if enabled, ok := typed["enabled"].(bool); ok && !enabled {
			return true
		}
		if mode := strings.ToLower(strings.TrimSpace(asString(typed["mode"]))); mode == "none" || mode == "off" {
			return true
		}
		if budget, ok := numeric(typed["budget_tokens"]); ok && budget <= 0 {
			return true
		}
	}
	return false
}
func asString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		if typed == math.Trunc(typed) {
			return strconvFormatInt(int64(typed))
		}
	}
	return ""
}

func firstString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if text := asString(values[key]); text != "" {
			return text
		}
	}
	return ""
}

func firstInt(values map[string]any, keys ...string) int64 {
	for _, key := range keys {
		if value, ok := numeric(values[key]); ok {
			return value
		}
	}
	return 0
}

func numeric(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int8:
		return int64(typed), true
	case int16:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	case uint:
		return int64(typed), true
	case uint8:
		return int64(typed), true
	case uint16:
		return int64(typed), true
	case uint32:
		return int64(typed), true
	case uint64:
		return int64(typed), true
	case float32:
		return int64(typed), true
	case float64:
		return int64(typed), true
	case json.Number:
		parsed, errParse := typed.Int64()
		return parsed, errParse == nil
	case string:
		parsed, errParse := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		return parsed, errParse == nil
	default:
		return 0, false
	}
}

func strconvFormatInt(value int64) string {
	return fmt.Sprintf("%d", value)
}
