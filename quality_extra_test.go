package main

import (
	"strings"
	"testing"
)

func TestGenerationWindowCorrectionBoundaries(t *testing.T) {
	cases := []struct {
		name      string
		duration  int64
		first     int64
		reasoning int64
		want      int64
	}{
		{name: "short reasoning window uses full duration", duration: 1200, first: 1000, reasoning: 10, want: 1200},
		{name: "long reasoning window subtracts ttft", duration: 3000, first: 1000, reasoning: 10, want: 2000},
		{name: "no reasoning never corrects", duration: 1200, first: 1000, reasoning: 0, want: 200},
		{name: "window equal to ttft does not correct", duration: 2000, first: 1000, reasoning: 10, want: 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := generationWindowMS(tc.duration, tc.first, tc.reasoning); got != tc.want {
				t.Fatalf("generation window = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestEncryptedContentDeduplication(t *testing.T) {
	encrypted := strings.Repeat("A", 2500)
	analyzer := NewStreamAnalyzer()
	payload := `data: {"type":"response.output_item.added","item":{"id":"rs_1","type":"reasoning","encrypted_content":"` + encrypted + `"}}` + "\n\n" +
		`data: {"type":"response.output_item.done","item":{"id":"rs_1","type":"reasoning","encrypted_content":"` + encrypted + `"}}` + "\n\n" +
		`data: {"type":"response.completed","response":{"usage":{"output_tokens":1100,"output_tokens_details":{"reasoning_tokens":1000}}}}` + "\n\n"
	analyzer.ObserveChunk([]byte(payload))
	sig := analyzer.Finalize(true)
	if sig.EncryptedBytes != 2500 {
		t.Fatalf("encrypted bytes = %d, want 2500 after deduplication", sig.EncryptedBytes)
	}
	if sig.HasThinking {
		t.Fatal("duplicated encrypted stub incorrectly satisfied the 4000-byte floor")
	}
}

func TestAnthropicThinkingStream(t *testing.T) {
	analyzer := NewStreamAnalyzer()
	analyzer.ObserveChunk([]byte("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n"))
	analyzer.ObserveChunk([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"plan carefully\"}}\n\n"))
	analyzer.ObserveChunk([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"final answer\"}}\n\n"))
	analyzer.ObserveChunk([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":30,\"output_tokens_details\":{\"thinking_tokens\":12}}}\n\n"))
	analyzer.ObserveChunk([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	sig := analyzer.Finalize(true)
	if !sig.HasThinking || !sig.HasReasoningDelta || !sig.Terminal {
		t.Fatalf("unexpected Anthropic signals: %#v", sig)
	}
	if sig.VisibleTokens != 3 || sig.OutputTokens != 30 || sig.ReasoningTokens != 12 {
		t.Fatalf("unexpected Anthropic counters: %#v", sig)
	}
	config := DefaultQualityConfig()
	config.SoftTPS = 1e9
	config.HardTPS = 2e9
	if verdict := classifyFinal(sig, config, true, 8, true); verdict.Class != ClassNone {
		t.Fatalf("unexpected verdict: %#v", verdict)
	}
}

func TestResponsesEncryptedThinking(t *testing.T) {
	encrypted := strings.Repeat("B", 4000)
	analyzer := NewStreamAnalyzer()
	analyzer.ObserveChunk([]byte(`data: {"type":"response.output_item.done","item":{"id":"rs_1","type":"reasoning","encrypted_content":"` + encrypted + `"}}` + "\n\n"))
	analyzer.ObserveChunk([]byte(`data: {"type":"response.output_text.delta","delta":"visible"}` + "\n\n"))
	analyzer.ObserveChunk([]byte(`data: {"type":"response.completed","response":{"usage":{"output_tokens":1100,"output_tokens_details":{"reasoning_tokens":1000}}}}` + "\n\n"))
	sig := analyzer.Finalize(true)
	if !sig.HasThinking {
		t.Fatalf("4000 encrypted bytes did not satisfy the floor: %#v", sig)
	}
	if verdict := classifyFinal(sig, DefaultQualityConfig(), true, 8, true); verdict.Class == ClassMissingThinking {
		t.Fatalf("encrypted thinking was incorrectly classified as missing: %#v", verdict)
	}
}

func TestMCPEmptyArrayIsReplaySafe(t *testing.T) {
	body := []byte(`{"model":"grok-4.5","mcp_servers":[],"messages":[{"role":"user","content":"hi"}]}`)
	if !safeReplayRequest(body, "codex", "grok-4.5") {
		t.Fatal("empty mcp_servers should not block replay")
	}
	body = []byte(`{"model":"grok-4.5","mcp_servers":[{"url":"https://example.com"}],"messages":[]}`)
	if safeReplayRequest(body, "codex", "grok-4.5") {
		t.Fatal("non-empty mcp_servers must block replay")
	}
}

func TestReasoningStartCommentDoesNotHideDataInSameEvent(t *testing.T) {
	analyzer := NewStreamAnalyzer()
	analyzer.ObserveChunk([]byte(": grok2api-reasoning-start\ndata: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n"))
	sig := analyzer.Finalize(true)
	if sig.VisibleTokens == 0 {
		t.Fatal("visible data after a reasoning-start comment was ignored")
	}
}
func TestPassiveTPSSkipsSamplesBelowMinimumOutput(t *testing.T) {
	config := DefaultQualityConfig()
	small := classifyFinal(Signals{
		Terminal:     true,
		FirstTokenMS: 100,
		DurationMS:   200,
		OutputTokens: 10,
	}, config, false, config.MinPassiveOutputTokens, false)
	if small.Class != ClassNone {
		t.Fatalf("small sample class = %q, want none", small.Class)
	}
	large := classifyFinal(Signals{
		Terminal:     true,
		FirstTokenMS: 100,
		DurationMS:   200,
		OutputTokens: 100,
	}, config, false, config.MinPassiveOutputTokens, false)
	if large.Class != ClassHardTPS {
		t.Fatalf("large sample class = %q, want hard_tps", large.Class)
	}
}
