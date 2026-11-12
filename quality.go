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
	ClassBufferedBurst             = "buffered_burst"
	ClassCipherDrool               = "cipher_drool"
	ClassFakeEncryptedDump         = "fake_encrypted_dump"
	ClassFastReasoningRatio        = "fast_reasoning_ratio_dump"
	ClassMissingThinking           = "missing_thinking"
	ClassHardTPS                   = "hard_tps"
	ClassSoftTPS                   = "soft_tps"
	ClassEmptyStream               = "empty_stream"
	ClassProbeError                = "probe_error"
	ClassProxyError                = "proxy_error"
	ClassCredentialInvalid         = "credential_invalid"
	ClassQuotaExhausted            = "quota_exhausted"
	ClassIgnored                   = "ignored"
	ClassInsufficientOutput        = "insufficient_output_tokens"
	ClassInsufficientGeneration    = "insufficient_generation_window"
	ClassExpectedMarkerMissing     = "expected_marker_missing"
	ClassNone                      = ""
	QualityActionWait              = "wait"
	QualityActionDeliver           = "deliver"
	QualityActionWithhold          = "withhold"
	qualityProtocolChat            = "chat"
	qualityProtocolResponses       = "responses"
	qualityProtocolAnthropic       = "anthropic"
	qualityReasoningSSEComment     = ": grok2api-reasoning-start"
	qualityReasoningEvidencePrefix = ": grok2api-reasoning-evidence"
	defaultQualityMinOutput        = int64(8)
	defaultQualityMinGenerationMS  = int64(1000)
	defaultQualityMaxVisible       = int64(32)
	defaultQualityMinReasoning     = int64(80)
	defaultQualityFastFlushMS      = int64(1000)
	defaultQualityFakeFlushMS      = int64(2000)
	defaultQualityCipherVisible    = int64(1024)
	defaultEncryptedBytes          = int64(256)
	defaultEncryptedBytesPerToken  = int64(4)
	thinkingGuardMinOutputTokens   = int64(64)
)

type Signals struct {
	Protocol          string
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
	FirstTokenKnown   bool
	DurationMS        int64
	GenerationMS      int64
	TPS               float64
	VisibleText       string
	ChunkCount        int
	ByteCount         int64
	EventCount        int
	ParsedEvents      int
	UnknownEvents     int
	ParseFailures     int
}

type QualityVerdict struct {
	Action  string
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
	buffer []byte
	event  string
}

// Feed consumes host model stream chunks. CPA forwards each executor SSE line
// without its trailing newline, so complete field lines must be handled as soon
// as they arrive instead of waiting for a newline that never comes.
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
		line := strings.TrimSuffix(string(p.buffer[:idx]), "\r")
		p.buffer = p.buffer[idx+1:]
		if ev, ok := p.processLine(line); ok {
			events = append(events, ev)
		}
	}
	if remainder := strings.TrimSuffix(string(p.buffer), "\r"); remainder != "" && sseLineComplete(remainder) {
		p.buffer = nil
		if ev, ok := p.processLine(remainder); ok {
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
	p.event = ""
	return events
}

// sseLineComplete reports whether an unterminated chunk is already a complete
// SSE field line. JSON is accepted only when it parses, so a payload split
// across chunks stays buffered.
func sseLineComplete(line string) bool {
	line = strings.TrimSpace(line)
	if line == "" {
		return false
	}
	if strings.HasPrefix(line, "event:") || strings.HasPrefix(line, ":") {
		return true
	}
	if data, ok := strings.CutPrefix(line, "data:"); ok {
		data = strings.TrimSpace(data)
		return data == "" || data == "[DONE]" || json.Valid([]byte(data))
	}
	if line == "[DONE]" {
		return true
	}
	if line[0] == '{' || line[0] == '[' {
		return json.Valid([]byte(line))
	}
	return false
}

func (p *SSEParser) processLine(line string) (SSEEvent, bool) {
	if line == "" {
		// A blank line terminates an SSE frame without carrying data.
		p.event = ""
		return SSEEvent{}, false
	}
	if strings.HasPrefix(line, ":") {
		return SSEEvent{Comment: strings.TrimSpace(strings.TrimPrefix(line, ":"))}, true
	}
	trimmed := strings.TrimSpace(line)
	if trimmed == "[DONE]" || ((strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[")) && json.Valid([]byte(trimmed))) {
		ev := SSEEvent{Event: p.event, Data: trimmed}
		p.event = ""
		return ev, true
	}
	field, value, _ := strings.Cut(line, ":")
	value = strings.TrimPrefix(value, " ")
	if field == "event" {
		p.event = value
		return SSEEvent{}, false
	}
	if field != "data" {
		return SSEEvent{}, false
	}
	data := strings.TrimSpace(value)
	if data == "" {
		return SSEEvent{}, false
	}
	ev := SSEEvent{Event: p.event, Data: data}
	p.event = ""
	return ev, true
}

type StreamAnalyzer struct {
	parser         SSEParser
	protocol       string
	streaming      bool
	startedAt      time.Time
	firstTokenAt   time.Time
	firstVisibleAt time.Time
	lastVisibleAt  time.Time
	terminalAt     time.Time
	visibleText    strings.Builder
	visibleRunes   int64
	aggregateRunes int64
	visibleDelta   bool
	reasoningDelta bool
	hasThinking    bool
	reasoningStart bool
	encryptedBytes int64
	minEncrypted   int64
	bytesPerToken  int64
	signals        Signals
}

func NewStreamAnalyzer() *StreamAnalyzer {
	return &StreamAnalyzer{
		streaming:     true,
		startedAt:     time.Now(),
		minEncrypted:  defaultEncryptedBytes,
		bytesPerToken: defaultEncryptedBytesPerToken,
	}
}

func NewStreamAnalyzerWithProtocol(protocol string) *StreamAnalyzer {
	analyzer := NewStreamAnalyzer()
	analyzer.protocol = normalizeQualityProtocol(protocol)
	return analyzer
}

func normalizeQualityProtocol(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch {
	case strings.Contains(normalized, "chat"):
		return qualityProtocolChat
	case strings.Contains(normalized, "message"), strings.Contains(normalized, "anthropic"):
		return qualityProtocolAnthropic
	case strings.Contains(normalized, "response"), strings.Contains(normalized, "codex"):
		return qualityProtocolResponses
	default:
		return normalized
	}
}

func (a *StreamAnalyzer) SetStreaming(value bool) {
	if a == nil {
		return
	}
	a.streaming = value
	a.signals.Streaming = value
}

func (a *StreamAnalyzer) SetProtocol(value string) {
	if a == nil {
		return
	}
	a.protocol = normalizeQualityProtocol(value)
}

func (a *StreamAnalyzer) SetEncryptedConfig(minBytes, bytesPerToken int64) {
	if a == nil {
		return
	}
	if minBytes > 0 {
		a.minEncrypted = minBytes
	}
	if bytesPerToken > 0 {
		a.bytesPerToken = bytesPerToken
	}
}

func (a *StreamAnalyzer) Text() string {
	if a == nil {
		return ""
	}
	return a.visibleText.String()
}

func (a *StreamAnalyzer) Preview() Signals {
	if a == nil {
		return Signals{}
	}
	return a.snapshot(false)
}

func (a *StreamAnalyzer) ObserveChunk(chunk []byte) {
	if a == nil || len(chunk) == 0 {
		return
	}
	a.signals.ChunkCount++
	a.signals.ByteCount += int64(len(chunk))
	for _, ev := range a.parser.Feed(chunk) {
		a.observeEvent(ev, time.Now())
	}
}

func (a *StreamAnalyzer) ObserveJSON(raw []byte) {
	if a == nil || len(raw) == 0 {
		return
	}
	a.observePayload(raw, "", time.Now())
}

func (a *StreamAnalyzer) observeEvent(ev SSEEvent, now time.Time) {
	comment := strings.TrimSpace(strings.ToLower(ev.Comment))
	if comment != "" {
		a.signals.EventCount++
		switch {
		case strings.HasPrefix(comment, strings.TrimPrefix(qualityReasoningSSEComment, ": ")):
			a.reasoningStart = true
			a.signals.ReasoningStarted = true
			a.markFirstToken(now)
		case strings.HasPrefix(comment, strings.TrimPrefix(qualityReasoningEvidencePrefix, ": ")):
			value := strings.TrimSpace(strings.TrimPrefix(ev.Comment, strings.TrimPrefix(qualityReasoningEvidencePrefix, ": ")))
			if count, err := strconv.ParseInt(value, 10, 64); err == nil && count > a.encryptedBytes {
				a.encryptedBytes = count
			}
			a.reasoningStart = true
			a.signals.ReasoningStarted = true
		}
	}
	data := strings.TrimSpace(ev.Data)
	if data == "" {
		return
	}
	a.signals.EventCount++
	if data == "[DONE]" {
		a.signals.Terminal = true
		a.terminalAt = now
		return
	}
	a.observePayload([]byte(data), ev.Event, now)
	if sseEventIsTerminal(ev.Event) {
		a.signals.Terminal = true
		a.terminalAt = now
	}
}

func sseEventIsTerminal(event string) bool {
	switch strings.ToLower(strings.TrimSpace(event)) {
	case "response.completed", "response.done", "response.incomplete", "response.failed", "message_stop":
		return true
	default:
		return false
	}
}

func (a *StreamAnalyzer) observePayload(raw []byte, fallbackEvent string, now time.Time) {
	var payload map[string]any
	if errUnmarshal := json.Unmarshal(raw, &payload); errUnmarshal != nil {
		a.signals.ParseFailures++
		return
	}
	a.signals.ParsedEvents++
	if fallbackEvent != "" && payload != nil && strings.TrimSpace(asString(payload["type"])) == "" {
		payload["type"] = fallbackEvent
	}
	protocol := a.protocol
	if protocol == "" {
		protocol = detectQualityProtocol(payload)
	} else if !qualityPayloadMatchesProtocol(payload, protocol) {
		if detected := detectQualityProtocol(payload); detected != "" {
			protocol = detected
		}
	}
	if protocol == "" {
		a.signals.UnknownEvents++
		return
	}
	switch protocol {
	case qualityProtocolChat:
		a.observeChat(payload, now)
	case qualityProtocolAnthropic:
		a.observeAnthropic(payload, now)
	default:
		a.observeResponses(payload, now)
	}
}

func detectQualityProtocol(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	if _, ok := payload["choices"]; ok {
		return qualityProtocolChat
	}
	eventName := strings.ToLower(strings.TrimSpace(asString(payload["type"])))
	if _, ok := payload["response"]; ok {
		return qualityProtocolResponses
	}
	if _, ok := payload["output"]; ok {
		return qualityProtocolResponses
	}
	if eventName == "response" || strings.HasPrefix(eventName, "response.") {
		return qualityProtocolResponses
	}
	if eventName == "message" || strings.HasPrefix(eventName, "content_block_") || strings.HasPrefix(eventName, "message_") {
		return qualityProtocolAnthropic
	}
	if _, ok := payload["content_block"]; ok {
		return qualityProtocolAnthropic
	}
	if _, ok := payload["delta"]; ok {
		return qualityProtocolAnthropic
	}
	if _, ok := payload["content"]; ok {
		if _, hasStop := payload["stop_reason"]; hasStop {
			return qualityProtocolAnthropic
		}
	}
	return ""
}

func qualityPayloadMatchesProtocol(payload map[string]any, protocol string) bool {
	if payload == nil {
		return true
	}
	switch protocol {
	case qualityProtocolChat:
		_, ok := payload["choices"]
		return ok
	case qualityProtocolAnthropic:
		eventName := strings.ToLower(strings.TrimSpace(asString(payload["type"])))
		if eventName == "message" || strings.HasPrefix(eventName, "content_block_") || strings.HasPrefix(eventName, "message_") {
			return true
		}
		if _, ok := payload["content_block"]; ok {
			return true
		}
		if _, ok := payload["delta"]; ok {
			return true
		}
		if _, ok := payload["content"]; ok {
			_, hasStop := payload["stop_reason"]
			return hasStop
		}
		return false
	case qualityProtocolResponses:
		eventName := strings.ToLower(strings.TrimSpace(asString(payload["type"])))
		if eventName == "response" || strings.HasPrefix(eventName, "response.") {
			return true
		}
		if _, ok := payload["response"]; ok {
			return true
		}
		_, ok := payload["output"]
		return ok
	default:
		return true
	}
}
func (a *StreamAnalyzer) observeChat(payload map[string]any, now time.Time) {
	a.observeUsage(payload)
	if choices, ok := payload["choices"].([]any); ok {
		for _, rawChoice := range choices {
			choice, _ := rawChoice.(map[string]any)
			if choice == nil {
				continue
			}
			if message, okMessage := choice["message"].(map[string]any); okMessage {
				a.observeChatMessage(message, now)
			}
			if delta, okDelta := choice["delta"].(map[string]any); okDelta {
				a.observeChatDelta(delta, now)
			}
			if finish := strings.TrimSpace(asString(choice["finish_reason"])); finish != "" {
				a.signals.Terminal = true
				a.terminalAt = now
			}
		}
	}
}

func (a *StreamAnalyzer) observeChatDelta(delta map[string]any, now time.Time) {
	if text := firstString(delta, "reasoning", "reasoning_content", "thinking_content"); strings.TrimSpace(text) != "" {
		a.addReasoningText(text)
	}
	if content, ok := delta["content"]; ok {
		a.addContentParts(content, now)
	}
	if toolCalls, ok := delta["tool_calls"].([]any); ok && len(toolCalls) > 0 {
		a.signals.SemanticOutput = true
		a.markFirstToken(now)
	}
	if functionCall := delta["function_call"]; functionCall != nil {
		a.signals.SemanticOutput = true
		a.markFirstToken(now)
	}
}

func (a *StreamAnalyzer) observeChatMessage(message map[string]any, now time.Time) {
	if text := firstString(message, "reasoning", "reasoning_content", "thinking_content"); strings.TrimSpace(text) != "" {
		a.addReasoningText(text)
	}
	if content, ok := message["content"]; ok {
		if a.streaming {
			a.observeResponsesMessageContent(content, now)
		} else {
			a.addContentParts(content, now)
		}
	}
	if toolCalls, ok := message["tool_calls"].([]any); ok && len(toolCalls) > 0 {
		a.signals.SemanticOutput = true
		a.markFirstToken(now)
	}
}

func (a *StreamAnalyzer) observeResponses(payload map[string]any, now time.Time) {
	eventName := strings.ToLower(strings.TrimSpace(asString(payload["type"])))
	if eventName == "" {
		if _, ok := payload["output"]; ok {
			eventName = "response.completed"
		}
	}
	a.observeUsage(payload)
	switch eventName {
	case "response.completed", "response.incomplete", "response.failed", "response.done":
		a.signals.Terminal = true
		a.terminalAt = now
	case "response.reasoning_text.delta", "response.reasoning_summary_text.delta":
		a.addReasoningText(asString(payload["delta"]))
	case "response.output_text.delta":
		a.addVisibleText(asString(payload["delta"]), now)
	case "response.output_text.done":
		if !a.visibleDelta {
			a.addVisibleText(asString(payload["text"]), now)
		}
	case "response.function_call_arguments.delta", "response.custom_tool_call_input.delta", "response.mcp_call_arguments.delta":
		if strings.TrimSpace(asString(payload["delta"])) != "" {
			a.signals.SemanticOutput = true
			a.markFirstToken(now)
		}
	case "response.output_item.added", "response.output_item.done":
		a.observeResponsesItem(payload, now)
	}
	if response, ok := payload["response"].(map[string]any); ok {
		if output, okOutput := response["output"].([]any); okOutput {
			var aggregateRunes int64
			for _, rawItem := range output {
				item, _ := rawItem.(map[string]any)
				if item != nil {
					aggregateRunes += a.observeResponsesItemMap(item, now)
				}
			}
			a.aggregateRunes = maxInt64(a.aggregateRunes, aggregateRunes)
		}
		a.observeUsage(response)
	}
}

func (a *StreamAnalyzer) observeResponsesItem(payload map[string]any, now time.Time) {
	item, _ := payload["item"].(map[string]any)
	if item == nil {
		return
	}
	runes := a.observeResponsesItemMap(item, now)
	a.aggregateRunes = maxInt64(a.aggregateRunes, runes)
}

func (a *StreamAnalyzer) observeResponsesItemMap(item map[string]any, now time.Time) int64 {
	itemType := strings.ToLower(strings.TrimSpace(asString(item["type"])))
	switch itemType {
	case "reasoning":
		if strings.TrimSpace(asString(item["id"])) != "" {
			a.reasoningStart = true
			a.signals.ReasoningStarted = true
		}
		if enc := asString(item["encrypted_content"]); strings.TrimSpace(enc) != "" {
			a.addEncrypted(enc)
		}
		a.observeResponsesReasoningContent(item["summary"])
		a.observeResponsesReasoningContent(item["content"])
		return 0
	case "message":
		return a.observeResponsesMessageContent(item["content"], now)
	case "":
		return 0
	default:
		a.signals.SemanticOutput = true
		a.markFirstToken(now)
		return 0
	}
}

func (a *StreamAnalyzer) observeResponsesReasoningContent(value any) {
	parts, ok := value.([]any)
	if !ok {
		return
	}
	for _, rawPart := range parts {
		part, _ := rawPart.(map[string]any)
		if part == nil {
			continue
		}
		a.addReasoningText(asString(part["text"]))
	}
}

func (a *StreamAnalyzer) observeResponsesMessageContent(value any, now time.Time) int64 {
	parts, ok := value.([]any)
	if !ok {
		if text := asString(value); strings.TrimSpace(text) != "" {
			return a.noteAggregateVisibleText(text, now, "")
		}
		return 0
	}
	var runes int64
	for _, rawPart := range parts {
		part, _ := rawPart.(map[string]any)
		if part == nil {
			continue
		}
		kind := strings.ToLower(strings.TrimSpace(asString(part["type"])))
		switch kind {
		case "output_text", "text", "refusal":
			runes += a.noteAggregateVisibleText(asString(part["text"]), now, kind)
		case "reasoning_text", "summary_text":
			a.addReasoningText(asString(part["text"]))
		case "tool_call", "function_call":
			a.signals.SemanticOutput = true
			a.markFirstToken(now)
		default:
			if kind != "" {
				a.signals.SemanticOutput = true
			}
		}
	}
	return runes
}

func (a *StreamAnalyzer) noteAggregateVisibleText(text string, now time.Time, kind string) int64 {
	_ = now
	_ = kind
	if text == "" {
		return 0
	}
	a.signals.SemanticOutput = true
	if !a.visibleDelta {
		a.visibleText.WriteString(text)
	}
	return int64(utf8.RuneCountInString(text))
}

func (a *StreamAnalyzer) observeAnthropic(payload map[string]any, now time.Time) {
	eventName := strings.ToLower(strings.TrimSpace(asString(payload["type"])))
	a.observeUsage(payload)
	switch eventName {
	case "message_stop":
		a.signals.Terminal = true
		a.terminalAt = now
	case "message":
		if content, ok := payload["content"]; ok {
			a.observeAnthropicContent(content, now)
		}
		if stopReason := strings.TrimSpace(asString(payload["stop_reason"])); stopReason != "" {
			a.signals.Terminal = true
			a.terminalAt = now
		}
	case "content_block_start":
		block, _ := payload["content_block"].(map[string]any)
		if block == nil {
			return
		}
		switch strings.ToLower(asString(block["type"])) {
		case "thinking":
			a.reasoningStart = true
			a.signals.ReasoningStarted = true
			if text := asString(block["thinking"]); strings.TrimSpace(text) != "" {
				a.addReasoningText(text)
			}
		case "redacted_thinking":
			a.reasoningStart = true
			a.signals.ReasoningStarted = true
			a.addEncrypted(asString(block["data"]))
		case "text":
			a.addVisibleText(asString(block["text"]), now)
			a.signals.SemanticOutput = true
		case "tool_use", "server_tool_use":
			a.signals.SemanticOutput = true
			a.markFirstToken(now)
		}
	case "content_block_delta":
		delta, _ := payload["delta"].(map[string]any)
		if delta == nil {
			return
		}
		switch strings.ToLower(asString(delta["type"])) {
		case "thinking_delta":
			a.addReasoningText(firstString(delta, "thinking", "text"))
		case "signature_delta":
			a.reasoningStart = true
			a.signals.ReasoningStarted = true
			a.addEncrypted(asString(delta["signature"]))
		case "text_delta":
			a.addVisibleText(asString(delta["text"]), now)
		case "input_json_delta":
			if strings.TrimSpace(asString(delta["partial_json"])) != "" {
				a.signals.SemanticOutput = true
				a.markFirstToken(now)
			}
		}
	}
}
func (a *StreamAnalyzer) observeAnthropicContent(value any, now time.Time) {
	parts, ok := value.([]any)
	if !ok {
		return
	}
	for _, rawPart := range parts {
		part, _ := rawPart.(map[string]any)
		if part == nil {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(asString(part["type"]))) {
		case "thinking":
			a.reasoningStart = true
			a.signals.ReasoningStarted = true
			a.addReasoningText(firstString(part, "thinking", "text"))
		case "redacted_thinking":
			a.reasoningStart = true
			a.signals.ReasoningStarted = true
			a.addEncrypted(asString(part["data"]))
		case "text":
			a.addVisibleText(asString(part["text"]), now)
			a.signals.SemanticOutput = true
		case "tool_use", "server_tool_use":
			a.signals.SemanticOutput = true
			a.markFirstToken(now)
		}
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
			switch kind {
			case "output_text", "text":
				a.addVisibleText(asString(part["text"]), now)
			case "reasoning_text", "summary_text":
				a.addReasoningText(asString(part["text"]))
			case "tool_call", "function_call":
				a.signals.SemanticOutput = true
				a.markFirstToken(now)
			case "refusal":
				a.addVisibleText(asString(part["text"]), now)
				a.signals.SemanticOutput = true
			}
		}
	}
}

func (a *StreamAnalyzer) addReasoningText(text string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	a.hasThinking = true
	a.reasoningDelta = true
	a.signals.HasReasoningDelta = true
	a.signals.ReasoningStarted = true
	a.markFirstToken(time.Now())
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
	a.visibleRunes += int64(utf8.RuneCountInString(text))
	a.signals.VisibleRunes = maxInt64(a.signals.VisibleRunes, a.visibleRunes)
	a.signals.VisibleTokens = (a.signals.VisibleRunes + 3) / 4
	a.markFirstToken(now)
}

func (a *StreamAnalyzer) addEncrypted(value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	if size := int64(len([]byte(value))); size > a.encryptedBytes {
		a.encryptedBytes = size
	}
	a.signals.EncryptedBytes = a.encryptedBytes
}

func (a *StreamAnalyzer) markFirstToken(now time.Time) {
	if a.firstTokenAt.IsZero() {
		a.firstTokenAt = now
	}
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
	if value := firstInt(usage, "completion_tokens", "output_tokens"); value > a.signals.OutputTokens {
		a.signals.OutputTokens = value
	}
	for _, key := range []string{"completion_tokens_details", "output_tokens_details"} {
		if detail, ok := usage[key].(map[string]any); ok {
			if value := firstInt(detail, "reasoning_tokens", "thinking_tokens"); value > a.signals.ReasoningTokens {
				a.signals.ReasoningTokens = value
			}
		}
	}
	if value := firstInt(usage, "reasoning_tokens", "thinking_tokens"); value > a.signals.ReasoningTokens {
		a.signals.ReasoningTokens = value
	}
}

func (a *StreamAnalyzer) snapshot(final bool) Signals {
	if a == nil {
		return Signals{}
	}
	sig := a.signals
	sig.Protocol = a.protocol
	sig.Streaming = a.streaming
	sig.EncryptedBytes = a.encryptedBytes
	sig.ReasoningStarted = a.reasoningStart || sig.ReasoningStarted || sig.HasThinking
	floor := encryptedFloorWithConfig(sig.ReasoningTokens, a.minEncrypted, a.bytesPerToken)
	sig.EncryptedFloor = floor
	sig.HasThinking = a.reasoningDelta || sig.HasReasoningDelta || (sig.EncryptedBytes > 0 && sig.EncryptedBytes >= floor)
	sig.VisibleRunes = maxInt64(sig.VisibleRunes, a.aggregateRunes)
	sig.VisibleTokens = (sig.VisibleRunes + 3) / 4
	if sig.OutputTokens <= 0 && final {
		sig.OutputTokens = sig.VisibleTokens
	}
	if a.firstTokenAt.IsZero() {
		sig.FirstTokenMS = 0
		sig.FirstTokenKnown = false
	} else {
		sig.FirstTokenMS = durationMS(a.startedAt, a.firstTokenAt)
		sig.FirstTokenKnown = true
	}
	end := a.terminalAt
	if end.IsZero() {
		end = time.Now()
	}
	if !a.firstVisibleAt.IsZero() {
		sig.FirstVisible = true
		sig.VisibleFlushMS = durationMS(a.firstVisibleAt, end)
		if sig.VisibleFlushMS < 0 {
			sig.VisibleFlushMS = 0
		}
	}
	sig.DurationMS = durationMS(a.startedAt, end)
	sig.GenerationMS = generationWindowMS(sig.DurationMS, sig.FirstTokenMS, sig.ReasoningTokens)
	sig.TPS = tps(sig.OutputTokens, sig.GenerationMS)
	sig.VisibleText = a.visibleText.String()
	sig.ChunkCount = a.signals.ChunkCount
	sig.ByteCount = a.signals.ByteCount
	sig.EventCount = a.signals.EventCount
	sig.ParsedEvents = a.signals.ParsedEvents
	sig.UnknownEvents = a.signals.UnknownEvents
	sig.ParseFailures = a.signals.ParseFailures
	return sig
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
	return a.snapshot(true)
}
func encryptedFloor(reasoningTokens int64, cfg QualityConfig) int64 {
	return encryptedFloorWithConfig(reasoningTokens, cfg.MinEncryptedBytes, cfg.EncryptedBytesPerReasoningToken)
}

func encryptedFloorWithConfig(reasoningTokens, minBytes, bytesPerToken int64) int64 {
	if minBytes <= 0 {
		minBytes = defaultEncryptedBytes
	}
	if bytesPerToken <= 0 {
		bytesPerToken = defaultEncryptedBytesPerToken
	}
	floor := minBytes
	if reasoningTokens > 0 {
		if reasoningTokens > math.MaxInt64/bytesPerToken {
			return math.MaxInt64
		}
		if need := reasoningTokens * bytesPerToken; need > floor {
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
	if durationMS <= 0 {
		return 0
	}
	if firstTokenMS < 0 {
		firstTokenMS = 0
	}
	if firstTokenMS >= durationMS {
		return 0
	}
	generationMS := durationMS - firstTokenMS
	if reasoningTokens > 0 && generationMS < firstTokenMS && generationMS < defaultQualityMinGenerationMS {
		return durationMS
	}
	return generationMS
}

func qualityMeetsEncryptedFloor(sig Signals) bool {
	if sig.EncryptedBytes <= 0 {
		return false
	}
	floor := sig.EncryptedFloor
	if floor <= 0 {
		floor = encryptedFloorWithConfig(sig.ReasoningTokens, defaultEncryptedBytes, defaultEncryptedBytesPerToken)
	}
	return sig.EncryptedBytes >= floor
}

func qualityHasDumpBill(sig Signals) bool {
	return qualityMeetsEncryptedFloor(sig) || sig.ReasoningTokens >= defaultQualityMinReasoning
}

func qualityFastFlush(sig Signals, limitMS int64) bool {
	return sig.FirstVisible && sig.VisibleFlushMS >= 0 && sig.VisibleFlushMS < limitMS
}

func qualityIsBurstDump(sig Signals, minOutput int64) bool {
	_ = minOutput
	if sig.HasReasoningDelta {
		return false
	}
	shortVisible := sig.VisibleTokens > 0 && sig.VisibleTokens < defaultQualityMaxVisible
	heavyReasoning := sig.ReasoningTokens >= defaultQualityMinReasoning
	if sig.HoldExpired && shortVisible && heavyReasoning {
		return true
	}
	return qualityFastFlush(sig, defaultQualityFastFlushMS) && qualityHasDumpBill(sig)
}

func qualityIsCipherDrool(sig Signals, minOutput int64) bool {
	if minOutput <= 0 {
		minOutput = defaultQualityMinOutput
	}
	if sig.HasReasoningDelta || sig.ReasoningTokens > 0 || sig.EncryptedBytes <= 0 {
		return false
	}
	if sig.VisibleTokens >= defaultQualityCipherVisible {
		return true
	}
	return sig.Terminal && sig.VisibleTokens >= minOutput
}

func qualityIsFakeEncryptedDump(sig Signals, minOutput int64) bool {
	_ = minOutput
	if sig.HasReasoningDelta {
		return false
	}
	if !qualityFastFlush(sig, defaultQualityFakeFlushMS) {
		return false
	}
	return qualityHasDumpBill(sig)
}

func qualityIsFastReasoningRatioDump(sig Signals) bool {
	if !sig.HasReasoningDelta {
		return false
	}
	if !qualityFastFlush(sig, defaultQualityFakeFlushMS) {
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

func ClassifyQualityHold(sig Signals, minOutput int64) QualityVerdict {
	if minOutput <= 0 {
		minOutput = defaultQualityMinOutput
	}
	if sig.EncryptedFloor <= 0 {
		sig.EncryptedFloor = encryptedFloorWithConfig(sig.ReasoningTokens, defaultEncryptedBytes, defaultEncryptedBytesPerToken)
	}
	if sig.EncryptedBytes > 0 && sig.EncryptedBytes >= sig.EncryptedFloor {
		sig.HasThinking = true
	}
	switch {
	case qualityIsBurstDump(sig, minOutput):
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassBufferedBurst, Reason: "short generation window dumped output", Signals: sig}
	case qualityIsCipherDrool(sig, minOutput):
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassCipherDrool, Reason: "cipher-only stream is dumping visible text", Signals: sig}
	case qualityIsFakeEncryptedDump(sig, minOutput):
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassFakeEncryptedDump, Reason: "encrypted or billed reasoning preceded a fast visible dump", Signals: sig}
	case qualityIsFastReasoningRatioDump(sig):
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassFastReasoningRatio, Reason: "reasoning tokens dominate a fast visible dump", Signals: sig}
	}
	if sig.HasThinking {
		if sig.HasReasoningDelta {
			return QualityVerdict{Action: QualityActionDeliver, Class: ClassNone, Reason: "real reasoning delta observed", Signals: sig}
		}
		if sig.Terminal {
			return QualityVerdict{Action: QualityActionDeliver, Class: ClassNone, Reason: "encrypted reasoning met the floor", Signals: sig}
		}
		if sig.VisibleTokens >= minOutput && sig.FirstVisible && sig.VisibleFlushMS >= defaultQualityFakeFlushMS {
			return QualityVerdict{Action: QualityActionDeliver, Class: ClassNone, Reason: "encrypted reasoning with sustained visible output", Signals: sig}
		}
		return QualityVerdict{Action: QualityActionWait, Class: ClassNone, Reason: "waiting for encrypted reasoning evidence", Signals: sig}
	}
	output := sig.VisibleTokens
	if output <= 0 {
		output = sig.OutputTokens
	}
	enough := output >= minOutput
	if sig.ReasoningStarted && !sig.Terminal && !sig.HoldExpired {
		return QualityVerdict{Action: QualityActionWait, Class: ClassNone, Reason: "waiting for reasoning evidence", Signals: sig}
	}
	if sig.Terminal {
		if output <= 0 {
			return QualityVerdict{Action: QualityActionWait, Class: ClassNone, Reason: "empty terminal stream", Signals: sig}
		}
		if enough {
			return QualityVerdict{Action: QualityActionWithhold, Class: ClassMissingThinking, Reason: "no real reasoning evidence", Signals: sig}
		}
		return QualityVerdict{Action: QualityActionDeliver, Class: ClassNone, Reason: "short response below thinking threshold", Signals: sig}
	}
	if enough {
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassMissingThinking, Reason: "no real reasoning evidence", Signals: sig}
	}
	if sig.HoldExpired {
		if output <= 0 {
			return QualityVerdict{Action: QualityActionWait, Class: ClassNone, Reason: "hold expired without output", Signals: sig}
		}
		if enough {
			return QualityVerdict{Action: QualityActionWithhold, Class: ClassMissingThinking, Reason: "no real reasoning evidence", Signals: sig}
		}
		return QualityVerdict{Action: QualityActionDeliver, Class: ClassNone, Reason: "short response below thinking threshold", Signals: sig}
	}
	return QualityVerdict{Action: QualityActionWait, Class: ClassNone, Reason: "waiting for enough output", Signals: sig}
}
func isSemanticOnly(sig Signals) bool {
	return sig.SemanticOutput && !sig.HasThinking && sig.VisibleTokens == 0
}

func isEmptyStream(sig Signals) bool {
	return sig.Terminal && !sig.HasThinking && sig.ReasoningTokens <= 0 && sig.OutputTokens <= 0 && sig.VisibleTokens <= 0 && !sig.SemanticOutput
}

func classifyOutputSpeed(sig Signals, cfg QualityConfig, failClosed bool) (string, float64, int64) {
	genMS := generationWindowMS(sig.DurationMS, sig.FirstTokenMS, sig.ReasoningTokens)
	if genMS <= 0 || sig.OutputTokens <= 0 {
		return ClassNone, 0, genMS
	}
	speed := tps(sig.OutputTokens, genMS)
	if failClosed && cfg.MinGenerationMS > 0 && genMS < cfg.MinGenerationMS && speed >= cfg.SoftTPS {
		return ClassBufferedBurst, speed, genMS
	}
	if speed >= cfg.HardTPS {
		return ClassHardTPS, speed, genMS
	}
	if speed >= cfg.SoftTPS {
		return ClassSoftTPS, speed, genMS
	}
	return ClassNone, speed, genMS
}

func classifyFinal(sig Signals, cfg QualityConfig, reasoningRequired bool, minOutput int64, streaming bool) QualityVerdict {
	sig.EncryptedFloor = encryptedFloor(sig.ReasoningTokens, cfg)
	if sig.EncryptedBytes > 0 && sig.EncryptedBytes >= sig.EncryptedFloor {
		sig.HasThinking = true
	}
	if sig.OutputTokens <= 0 {
		sig.OutputTokens = sig.VisibleTokens
	}
	sig.GenerationMS = generationWindowMS(sig.DurationMS, sig.FirstTokenMS, sig.ReasoningTokens)
	sig.TPS = tps(sig.OutputTokens, sig.GenerationMS)
	sig.Streaming = streaming
	if isEmptyStream(sig) {
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassEmptyStream, Reason: "upstream stream completed without content", Signals: sig}
	}
	if streaming {
		hold := ClassifyQualityHold(sig, minOutput)
		if hold.Action == QualityActionWithhold {
			if hold.Class == ClassMissingThinking && !reasoningRequired {
				hold.Action = QualityActionDeliver
				hold.Class = ClassNone
				hold.Reason = "reasoning was disabled for this request"
			} else {
				return hold
			}
		}
		sig = hold.Signals
		if hold.Action == QualityActionWait {
			if sig.SemanticOutput {
				hold.Action = QualityActionDeliver
				hold.Class = ClassNone
			} else {
				return QualityVerdict{Action: QualityActionWithhold, Class: ClassEmptyStream, Reason: "upstream stream completed without content", Signals: sig}
			}
		}
	}
	if reasoningRequired && !sig.HasThinking && !isSemanticOnly(sig) {
		output := sig.VisibleTokens
		if output <= 0 {
			output = sig.OutputTokens
		}
		if output >= minOutput {
			return QualityVerdict{Action: QualityActionWithhold, Class: ClassMissingThinking, Reason: "no real reasoning evidence", Signals: sig}
		}
	}
	class, speed, gen := classifyOutputSpeed(sig, cfg, cfg.FailClosed)
	sig.GenerationMS = gen
	sig.TPS = speed
	if class != ClassNone {
		return QualityVerdict{Action: QualityActionWithhold, Class: class, Reason: outputSpeedReason(class), Signals: sig}
	}
	return QualityVerdict{Action: QualityActionDeliver, Class: ClassNone, Reason: "within_threshold", Signals: sig}
}

func probeStreamUnparsed(sig Signals) bool {
	return sig.ByteCount > 0 &&
		sig.ParsedEvents == 0 &&
		sig.VisibleTokens <= 0 &&
		sig.OutputTokens <= 0 &&
		sig.ReasoningTokens <= 0 &&
		!sig.SemanticOutput
}

func classifyProbeResult(sig Signals, cfg QualityConfig, expectedMatched, thinkingRequired bool) QualityVerdict {
	if probeStreamUnparsed(sig) {
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassProbeError, Reason: "probe stream produced no parseable events", Signals: sig}
	}
	if !sig.Terminal {
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassProbeError, Reason: "probe stream ended without a terminal event", Signals: sig}
	}
	if !expectedMatched {
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassExpectedMarkerMissing, Reason: "expected marker missing", Signals: sig}
	}
	output := sig.OutputTokens
	if output <= 0 {
		output = sig.VisibleTokens
	}
	if output < cfg.MinPassiveOutputTokens {
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassInsufficientOutput, Reason: "insufficient output tokens", Signals: sig}
	}
	if thinkingRequired && output >= thinkingGuardMinOutputTokens && sig.ReasoningTokens <= 0 {
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassMissingThinking, Reason: "missing thinking", Signals: sig}
	}
	class, speed, gen := classifyOutputSpeed(sig, cfg, cfg.FailClosed)
	sig.GenerationMS = gen
	sig.TPS = speed
	if class == ClassNone && cfg.FailClosed && gen < cfg.MinGenerationMS {
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassInsufficientGeneration, Reason: "insufficient generation window", Signals: sig}
	}
	if class != ClassNone {
		return QualityVerdict{Action: QualityActionWithhold, Class: class, Reason: outputSpeedReason(class), Signals: sig}
	}
	return QualityVerdict{Action: QualityActionDeliver, Class: ClassNone, Reason: "within_threshold", Signals: sig}
}

func classifyPassiveAudit(sig Signals, cfg QualityConfig) QualityVerdict {
	if !sig.Streaming {
		return QualityVerdict{Action: QualityActionDeliver, Class: ClassIgnored, Reason: "not_build_stream", Signals: sig}
	}
	if !sig.Terminal || !sig.FirstTokenKnown {
		return QualityVerdict{Action: QualityActionDeliver, Class: ClassIgnored, Reason: "missing_first_token", Signals: sig}
	}
	sig.GenerationMS = generationWindowMS(sig.DurationMS, sig.FirstTokenMS, sig.ReasoningTokens)
	sig.TPS = tps(sig.OutputTokens, sig.GenerationMS)
	if sig.GenerationMS <= 0 || sig.OutputTokens < cfg.MinPassiveOutputTokens {
		return QualityVerdict{Action: QualityActionDeliver, Class: ClassIgnored, Reason: "insufficient_output_tokens", Signals: sig}
	}
	class, speed, gen := classifyOutputSpeed(sig, cfg, cfg.FailClosed)
	sig.GenerationMS = gen
	sig.TPS = speed
	if class == ClassNone {
		return QualityVerdict{Action: QualityActionDeliver, Class: ClassNone, Reason: "within_threshold", Signals: sig}
	}
	return QualityVerdict{Action: QualityActionWithhold, Class: class, Reason: outputSpeedReason(class), Signals: sig}
}

func classifyNonStreamResult(sig Signals, cfg QualityConfig, reasoningRequired bool, minOutput int64) QualityVerdict {
	sig.Streaming = false
	sig.Terminal = true
	sig.EncryptedFloor = encryptedFloor(sig.ReasoningTokens, cfg)
	if sig.EncryptedBytes > 0 && sig.EncryptedBytes >= sig.EncryptedFloor {
		sig.HasThinking = true
	}
	if sig.OutputTokens <= 0 {
		sig.OutputTokens = sig.VisibleTokens
	}
	sig.GenerationMS = generationWindowMS(sig.DurationMS, sig.FirstTokenMS, sig.ReasoningTokens)
	sig.TPS = tps(sig.OutputTokens, sig.GenerationMS)
	if isEmptyStream(sig) {
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassEmptyStream, Reason: "upstream response completed without content", Signals: sig}
	}
	if reasoningRequired && !sig.HasThinking && !isSemanticOnly(sig) {
		output := sig.VisibleTokens
		if output <= 0 {
			output = sig.OutputTokens
		}
		if output >= minOutput {
			return QualityVerdict{Action: QualityActionWithhold, Class: ClassMissingThinking, Reason: "no real reasoning evidence", Signals: sig}
		}
	}
	class, speed, gen := classifyOutputSpeed(sig, cfg, false)
	sig.GenerationMS = gen
	sig.TPS = speed
	if class != ClassNone {
		return QualityVerdict{Action: QualityActionWithhold, Class: class, Reason: outputSpeedReason(class), Signals: sig}
	}
	if cfg.FailClosed && gen <= 0 && sig.OutputTokens >= minOutput {
		return QualityVerdict{Action: QualityActionWithhold, Class: ClassInsufficientGeneration, Reason: "insufficient generation window", Signals: sig}
	}
	return QualityVerdict{Action: QualityActionDeliver, Class: ClassNone, Reason: "within_threshold", Signals: sig}
}

func outputSpeedReason(class string) string {
	switch class {
	case ClassBufferedBurst:
		return "short generation window dumped output"
	case ClassHardTPS:
		return "output tokens per second exceeded hard threshold"
	case ClassSoftTPS:
		return "output tokens per second exceeded soft threshold"
	default:
		return ""
	}
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
		if raw, ok := payload[key]; ok && disabledReasoningValue(raw) {
			return false
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

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
