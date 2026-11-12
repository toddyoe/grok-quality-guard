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
