package main

import (
	"strings"
	"testing"
)

func TestReasoningStubAndUsageTokensAreNotThinking(t *testing.T) {
	analyzer := NewStreamAnalyzer()
	analyzer.ObserveChunk([]byte(": grok2api-reasoning-start\n\n"))
	analyzer.ObserveChunk([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"answer answer answer answer answer answer answer answer answer answer answer answer answer answer answer answer answer answer answer answer \"}}]}\n\n"))
	analyzer.ObserveChunk([]byte("data: {\"usage\":{\"completion_tokens\":80,\"completion_tokens_details\":{\"reasoning_tokens\":79}}}\n\n"))
	sig := analyzer.Finalize(true)
	if sig.HasThinking {
		t.Fatal("usage tokens or a reasoning stub must not count as thinking")
	}
	verdict := classifyFinal(sig, DefaultQualityConfig(), true, 32, true)
	if verdict.Class != ClassMissingThinking {
		t.Fatalf("class = %q, want %q", verdict.Class, ClassMissingThinking)
	}
}

func TestPlaintextReasoningDeltaCountsAsThinking(t *testing.T) {
	analyzer := NewStreamAnalyzer()
	analyzer.ObserveChunk([]byte("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"I should plan first\"}}]}\n\n"))
	analyzer.ObserveChunk([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n"))
	analyzer.ObserveChunk([]byte("data: {\"usage\":{\"completion_tokens\":20,\"completion_tokens_details\":{\"reasoning_tokens\":10}}}\n\n"))
	sig := analyzer.Finalize(true)
	if !sig.HasThinking || !sig.HasReasoningDelta {
		t.Fatalf("plaintext reasoning not detected: %#v", sig)
	}
	config := DefaultQualityConfig()
	config.SoftTPS = 1e9
	config.HardTPS = 2e9
	verdict := classifyFinal(sig, config, true, 8, true)
	if verdict.Class != ClassNone {
		t.Fatalf("class = %q, want none (%#v)", verdict.Class, sig)
	}
}

func TestEncryptedThinkingFloor(t *testing.T) {
	build := func(size int) Signals {
		encrypted := strings.Repeat("A", size)
		analyzer := NewStreamAnalyzer()
		analyzer.ObserveChunk([]byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\",\"encrypted_content\":\"" + encrypted + "\"}}\n\n"))
		analyzer.ObserveChunk([]byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"output_tokens\":1100,\"output_tokens_details\":{\"reasoning_tokens\":1000}}}}\n\n"))
		analyzer.ObserveChunk([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"visible\"}\n\n"))
		return analyzer.Finalize(true)
	}
	under := build(256)
	if under.HasThinking {
		t.Fatalf("256-byte stub must not satisfy 1000 reasoning tokens: %#v", under)
	}
	enough := build(4000)
	if !enough.HasThinking {
		t.Fatalf("4000-byte encrypted content should satisfy 1000 reasoning tokens: %#v", enough)
	}
}

func TestVisibleTokenEstimateUsesVisibleTextOnly(t *testing.T) {
	analyzer := NewStreamAnalyzer()
	analyzer.ObserveChunk([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"12345678\"}}]}\n\n"))
	sig := analyzer.Finalize(true)
	if sig.VisibleTokens != 2 {
		t.Fatalf("VisibleTokens = %d, want 2", sig.VisibleTokens)
	}
	if sig.OutputTokens != 2 {
		t.Fatalf("OutputTokens fallback = %d, want 2", sig.OutputTokens)
	}
}

func TestTPSAndBurstClassification(t *testing.T) {
	config := DefaultQualityConfig()
	sig := Signals{
		Streaming:       true,
		Terminal:        true,
		FirstTokenMS:    100,
		DurationMS:      600,
		OutputTokens:    1000,
		ReasoningTokens: 100,
		VisibleTokens:   20,
		EncryptedBytes:  400,
		EncryptedFloor:  400,
		HasThinking:     true,
		FirstVisible:    true,
		VisibleFlushMS:  100,
	}
	verdict := classifyFinal(sig, config, false, 8, true)
	if verdict.Class != ClassBufferedBurst {
		t.Fatalf("class = %q, want buffered_burst (%#v)", verdict.Class, verdict.Signals)
	}
	hard := classifyFinal(Signals{
		Terminal:     true,
		FirstTokenMS: 100,
		DurationMS:   3000,
		OutputTokens: 4000,
	}, config, false, 8, false)
	if hard.Class != ClassHardTPS {
		t.Fatalf("class = %q, want hard_tps", hard.Class)
	}
}

func TestDumpClassesHavePriority(t *testing.T) {
	config := DefaultQualityConfig()
	cipher := Signals{
		Streaming:      true,
		Terminal:       true,
		VisibleTokens:  1024,
		EncryptedBytes: 400,
		EncryptedFloor: 256,
	}
	if got := classifyFinal(cipher, config, false, 8, true).Class; got != ClassCipherDrool {
		t.Fatalf("cipher class = %q, want cipher_drool", got)
	}

	fake := Signals{
		Streaming:      true,
		Terminal:       true,
		FirstVisible:   true,
		VisibleFlushMS: 1500,
		VisibleTokens:  4,
		EncryptedBytes: 4000,
		EncryptedFloor: 4000,
	}
	if got := classifyFinal(fake, config, false, 8, true).Class; got != ClassFakeEncryptedDump {
		t.Fatalf("fake encrypted class = %q, want fake_encrypted_dump", got)
	}

	ratio := Signals{
		Streaming:         true,
		Terminal:          true,
		FirstVisible:      true,
		VisibleFlushMS:    800,
		VisibleTokens:     400,
		OutputTokens:      1000,
		ReasoningTokens:   850,
		HasReasoningDelta: true,
		HasThinking:       true,
	}
	if got := classifyFinal(ratio, config, true, 8, true).Class; got != ClassFastReasoningRatio {
		t.Fatalf("ratio class = %q, want fast_reasoning_ratio_dump", got)
	}
}

func TestEmptyStream(t *testing.T) {
	verdict := classifyFinal(Signals{Terminal: true, Streaming: true}, DefaultQualityConfig(), true, 8, true)
	if verdict.Class != ClassEmptyStream {
		t.Fatalf("class = %q, want empty_stream", verdict.Class)
	}
}

func TestSSEParserHandlesMissingTrailingNewline(t *testing.T) {
	analyzer := NewStreamAnalyzer()
	analyzer.ObserveChunk([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}"))
	sig := analyzer.Finalize(true)
	if sig.VisibleTokens != 1 {
		t.Fatalf("VisibleTokens = %d, want 1", sig.VisibleTokens)
	}
}

func TestReasoningDisabledBody(t *testing.T) {
	if reasoningRequired([]byte(`{"reasoning_effort":"none"}`)) {
		t.Fatal("reasoning_effort=none should disable reasoning requirement")
	}
	if !reasoningRequired([]byte(`{"reasoning":{"effort":"high"}}`)) {
		t.Fatal("reasoning effort high should require thinking evidence")
	}
}

func TestReasoningEligibilityUsesDynamicModelRules(t *testing.T) {
	reasoningModels := []string{
		"Build/grok-4.6-low",
		"grok-4.6-xhigh",
		"grok-4.6-max",
		"grok-4.8",
		"grok-5.0-xhigh",
		"grok-4.7-build-fast",
		"grok-3-mini-fast",
	}
	for _, model := range reasoningModels {
		if !reasoningRequiredForModel(model, []byte(`{"stream":true}`)) {
			t.Fatalf("model %q should require thinking evidence", model)
		}
	}

	nonReasoningModels := []string{
		"grok-composer-2.5-fast",
		"grok-composer-3.0-fast",
		"grok-build-0.1",
		"grok-imagine-image-2.0",
		"grok-4.20-0309-non-reasoning",
		"grok-4.8-non-reasoning",
		"grok-4.6-none",
		"grok-2",
		"custom-grok",
	}
	for _, model := range nonReasoningModels {
		if reasoningRequiredForModel(model, []byte(`{"stream":true}`)) {
			t.Fatalf("model %q must not use the thinking gate", model)
		}
	}

	if reasoningRequiredForModel("grok-4.6", []byte(`{"reasoning_effort":"none"}`)) {
		t.Fatal("explicitly disabled reasoning must bypass the thinking gate")
	}
	if reasoningRequiredForModel("custom-grok", []byte(`{"reasoning_effort":"high"}`)) {
		t.Fatal("unknown models must stay outside the thinking gate, matching grok2api")
	}
	if !reasoningRequiredForModel("vendor-alias", []byte(`{"model":"grok-4.6","stream":true}`)) {
		t.Fatal("known body model should resolve an unknown requested alias")
	}
}

func TestProtocolAliasesAndNonStreamThinkingParsing(t *testing.T) {
	if got := normalizeQualityProtocol("openai"); got != qualityProtocolChat {
		t.Fatalf("openai protocol = %q, want %q", got, qualityProtocolChat)
	}
	if got := normalizeQualityProtocol("claude"); got != qualityProtocolAnthropic {
		t.Fatalf("claude protocol = %q, want %q", got, qualityProtocolAnthropic)
	}

	chat := NewStreamAnalyzerWithProtocol("openai")
	chat.SetStreaming(false)
	chat.ObserveJSON([]byte(`{"choices":[{"message":{"content":"answer","reasoning_content":"thought"}}],"usage":{"completion_tokens":10,"completion_tokens_details":{"reasoning_tokens":3}}}`))
	chatSignals := chat.Finalize(true)
	if !chatSignals.HasThinking || chatSignals.VisibleTokens == 0 {
		t.Fatalf("non-stream chat thinking parsing failed: %#v", chatSignals)
	}

	claude := NewStreamAnalyzerWithProtocol("claude")
	claude.SetStreaming(false)
	claude.ObserveJSON([]byte(`{"type":"message","content":[{"type":"thinking","thinking":"thought"},{"type":"text","text":"answer"}],"usage":{"output_tokens":10}}`))
	claudeSignals := claude.Finalize(true)
	if !claudeSignals.HasThinking || claudeSignals.VisibleTokens == 0 {
		t.Fatalf("non-stream claude thinking parsing failed: %#v", claudeSignals)
	}
}

func TestSSEParserHandlesHostLineChunks(t *testing.T) {
	analyzer := NewStreamAnalyzerWithProtocol(qualityProtocolResponses)
	analyzer.ObserveChunk([]byte("event: response.output_text.delta"))
	analyzer.ObserveChunk([]byte(`data: {"type":"response.output_text.delta","delta":"QUALITY_OK"}`))
	analyzer.ObserveChunk([]byte("event: response.completed"))
	analyzer.ObserveChunk([]byte(`data: {"response":{"usage":{"output_tokens":40,"output_tokens_details":{"reasoning_tokens":10}}}}`))
	sig := analyzer.Finalize(true)
	if sig.VisibleText != "QUALITY_OK" {
		t.Fatalf("VisibleText = %q, want QUALITY_OK", sig.VisibleText)
	}
	if !sig.Terminal || sig.ParsedEvents != 2 || sig.OutputTokens != 40 || sig.ReasoningTokens != 10 {
		t.Fatalf("unexpected line-chunk signals: %#v", sig)
	}
	if verdict := classifyProbeResult(sig, DefaultQualityConfig(), true, true); verdict.Class != ClassNone {
		t.Fatalf("line-chunk probe class = %q, want none", verdict.Class)
	}
}

func TestSSEParserJoinsSplitFieldChunks(t *testing.T) {
	analyzer := NewStreamAnalyzer()
	analyzer.ObserveChunk([]byte("da"))
	analyzer.ObserveChunk([]byte(`ta: {"choices":[{"delta":{"content":"QUALITY_OK"}}]}`))
	analyzer.ObserveChunk([]byte(`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`))
	sig := analyzer.Finalize(true)
	if sig.VisibleText != "QUALITY_OK" || !sig.Terminal {
		t.Fatalf("split field chunks were not parsed: %#v", sig)
	}
}

func TestSSEParserHandlesBareJSONChatChunks(t *testing.T) {
	analyzer := NewStreamAnalyzerWithProtocol(qualityProtocolChat)
	analyzer.ObserveChunk([]byte(`{"choices":[{"delta":{"content":"QUALITY_OK"}}]}`))
	analyzer.ObserveChunk([]byte(`{"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"completion_tokens":40}}`))
	sig := analyzer.Finalize(true)
	if sig.VisibleText != "QUALITY_OK" || !sig.Terminal || sig.OutputTokens != 40 {
		t.Fatalf("bare JSON chat chunks were not parsed: %#v", sig)
	}
}

func TestResponsesNonStreamThinkingParsing(t *testing.T) {
	body := []byte(`{"id":"resp_1","object":"response","model":"grok-4.6","output":[{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"plan the answer"}]},{"id":"msg_1","type":"message","content":[{"type":"output_text","text":"final answer"}]}],"usage":{"input_tokens":10,"output_tokens":20,"output_tokens_details":{"reasoning_tokens":8}}}`)
	analyzer := NewStreamAnalyzerWithProtocol("openai-response")
	analyzer.SetStreaming(false)
	analyzer.ObserveJSON(body)
	signals := analyzer.Finalize(true)
	if !signals.HasThinking || !signals.HasReasoningDelta || signals.VisibleTokens == 0 {
		t.Fatalf("top-level Responses thinking parsing failed: %#v", signals)
	}
	verdict := classifyNonStreamResult(signals, DefaultQualityConfig(), true, 8)
	if verdict.Class != ClassNone {
		t.Fatalf("non-stream Responses with thinking was rejected: %#v", verdict)
	}
}

func TestResponsesReasoningSummaryDoneIsThinking(t *testing.T) {
	analyzer := NewStreamAnalyzerWithProtocol(qualityProtocolResponses)
	analyzer.SetStreaming(false)
	analyzer.ObserveChunk([]byte("data: {\"type\":\"response.reasoning_summary_text.done\",\"text\":\"plan the answer\"}\n\n"))
	analyzer.ObserveChunk([]byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"output_tokens\":8}}}\n\n"))
	signals := analyzer.Finalize(true)
	if !signals.HasThinking || !signals.HasReasoningDelta {
		t.Fatalf("reasoning summary done text was not counted as thinking: %#v", signals)
	}
}

func TestResponsesNonStreamOutputTextShorthand(t *testing.T) {
	analyzer := NewStreamAnalyzerWithProtocol("openai-response")
	analyzer.SetStreaming(false)
	analyzer.ObserveJSON([]byte(`{"id":"resp_1","object":"response","output_text":"a visible answer","usage":{"output_tokens":20}}`))
	signals := analyzer.Finalize(true)
	if signals.VisibleTokens == 0 || isEmptyStream(signals) {
		t.Fatalf("output_text shorthand was treated as empty: %#v", signals)
	}
}
