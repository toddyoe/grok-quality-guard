package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestPassiveAuditIgnoresIneligibleSamples(t *testing.T) {
	config := DefaultQualityConfig()
	nonStream := classifyPassiveAudit(Signals{OutputTokens: 100, FirstTokenKnown: true}, config)
	if nonStream.Class != ClassIgnored || nonStream.Reason != "not_build_stream" {
		t.Fatalf("non-stream passive verdict = %#v", nonStream)
	}
	missingTTFT := classifyPassiveAudit(Signals{Streaming: true, Terminal: true, DurationMS: 1000, OutputTokens: 100}, config)
	if missingTTFT.Class != ClassIgnored || missingTTFT.Reason != "missing_first_token" {
		t.Fatalf("missing-TTFT passive verdict = %#v", missingTTFT)
	}
	short := classifyPassiveAudit(Signals{Streaming: true, Terminal: true, FirstTokenKnown: true, FirstTokenMS: 100, DurationMS: 1000, OutputTokens: 31}, config)
	if short.Class != ClassIgnored || short.Reason != "insufficient_output_tokens" {
		t.Fatalf("short passive verdict = %#v", short)
	}
	healthy := classifyPassiveAudit(Signals{Streaming: true, Terminal: true, FirstTokenKnown: true, FirstTokenMS: 100, DurationMS: 1000, OutputTokens: 100}, config)
	if healthy.Class != ClassNone || healthy.Reason != "within_threshold" {
		t.Fatalf("healthy passive verdict = %#v", healthy)
	}
}

func TestClassifyNonStreamQuality(t *testing.T) {
	config := DefaultQualityConfig()
	empty := classifyNonStreamResult(Signals{}, config, true, 8)
	if empty.Class != ClassEmptyStream {
		t.Fatalf("empty non-stream class = %q", empty.Class)
	}
	missing := classifyNonStreamResult(Signals{OutputTokens: 100, VisibleTokens: 100, DurationMS: 1000}, config, true, 8)
	if missing.Class != ClassMissingThinking {
		t.Fatalf("missing-thinking non-stream class = %q", missing.Class)
	}
	short := classifyNonStreamResult(Signals{OutputTokens: 4, VisibleTokens: 4, DurationMS: 1000}, config, true, 8)
	if short.Class != ClassNone {
		t.Fatalf("short non-stream class = %q, want none", short.Class)
	}
}

func TestReasoningEvidenceCommentSatisfiesCipherFloor(t *testing.T) {
	analyzer := NewStreamAnalyzer()
	analyzer.ObserveChunk([]byte(": grok2api-reasoning-evidence 1024\n\n"))
	analyzer.ObserveChunk([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"visible\"}}]}\n\n"))
	signals := analyzer.Finalize(true)
	if !signals.HasThinking || signals.EncryptedBytes != 1024 || signals.EncryptedFloor != 256 {
		t.Fatalf("reasoning evidence was not applied: %#v", signals)
	}

	short := NewStreamAnalyzer()
	short.ObserveChunk([]byte(": grok2api-reasoning-evidence 64\n\n"))
	shortSignals := short.Finalize(true)
	if shortSignals.HasThinking {
		t.Fatalf("short encrypted stub counted as thinking: %#v", shortSignals)
	}
}

func TestResponsesReasoningSummaryAndAggregateMessage(t *testing.T) {
	analyzer := NewStreamAnalyzerWithProtocol(qualityProtocolResponses)
	analyzer.ObserveChunk([]byte("data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"plan the answer\"}\n\n"))
	analyzer.ObserveChunk([]byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}}\n\n"))
	analyzer.ObserveChunk([]byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"output_tokens\":40,\"output_tokens_details\":{\"reasoning_tokens\":20}}}}\n\n"))
	signals := analyzer.Finalize(true)
	if !signals.HasThinking || !signals.HasReasoningDelta || !signals.SemanticOutput || !signals.Terminal {
		t.Fatalf("unexpected Responses signals: %#v", signals)
	}
	if signals.VisibleTokens != 2 || signals.OutputTokens != 40 || signals.ReasoningTokens != 20 {
		t.Fatalf("unexpected Responses counters: %#v", signals)
	}
	if signals.FirstVisible {
		t.Fatalf("aggregate message should not create a streaming visible flush: %#v", signals)
	}
}

func TestAnthropicNonStreamMessage(t *testing.T) {
	analyzer := NewStreamAnalyzerWithProtocol(qualityProtocolAnthropic)
	analyzer.SetStreaming(false)
	analyzer.ObserveJSON([]byte(`{"type":"message","content":[{"type":"thinking","thinking":"plan carefully"},{"type":"text","text":"answer"}],"stop_reason":"end_turn","usage":{"output_tokens":20,"output_tokens_details":{"thinking_tokens":5}}}`))
	signals := analyzer.Finalize(true)
	if !signals.HasThinking || !signals.HasReasoningDelta || !signals.SemanticOutput || !signals.Terminal {
		t.Fatalf("unexpected Anthropic non-stream signals: %#v", signals)
	}
	if signals.VisibleTokens != 2 || signals.OutputTokens != 20 || signals.ReasoningTokens != 5 {
		t.Fatalf("unexpected Anthropic non-stream counters: %#v", signals)
	}
}

func TestProbeThinkingGuardRequiresMinimumOutput(t *testing.T) {
	config := DefaultQualityConfig()
	below := classifyProbeResult(Signals{Terminal: true, OutputTokens: 63, FirstTokenMS: 100, DurationMS: 1100}, config, true, true)
	if below.Class != ClassNone {
		t.Fatalf("probe below thinking guard threshold class = %q", below.Class)
	}
	atThreshold := classifyProbeResult(Signals{Terminal: true, OutputTokens: 64, FirstTokenMS: 100, DurationMS: 1100}, config, true, true)
	if atThreshold.Class != ClassMissingThinking {
		t.Fatalf("probe at thinking guard threshold class = %q", atThreshold.Class)
	}
}

func TestProbeMarkerUsesLastNonEmptyLine(t *testing.T) {
	if !matchProbeMarker("intro\nQUALITY_OK\u00a0\n\n", "QUALITY_OK") {
		t.Fatal("last-line marker with unicode trailing whitespace did not match")
	}
	if matchProbeMarker("QUALITY_OK\nextra", "QUALITY_OK") {
		t.Fatal("marker outside the last non-empty line incorrectly matched")
	}
}

func TestFailClosedProbeClassification(t *testing.T) {
	config := DefaultQualityConfig()
	config.FailClosed = true
	burst := classifyProbeResult(Signals{Terminal: true, OutputTokens: 400, FirstTokenMS: 100, DurationMS: 600}, config, true, false)
	if burst.Class != ClassBufferedBurst {
		t.Fatalf("fail-closed burst class = %q", burst.Class)
	}
	insufficient := classifyProbeResult(Signals{Terminal: true, OutputTokens: 128, FirstTokenMS: 100, DurationMS: 600}, config, true, false)
	if insufficient.Class != ClassInsufficientGeneration {
		t.Fatalf("fail-closed generation class = %q", insufficient.Class)
	}
}

func TestIgnoredAndProbeErrorPreserveQualityEvidence(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(config *Config) { config.AutoAction = "observe" })
	initial := QualityVerdict{
		Class:  ClassMissingThinking,
		Reason: "no real reasoning evidence",
		Signals: Signals{
			OutputTokens: 100, VisibleTokens: 100, FirstTokenMS: 100, DurationMS: 1000,
			GenerationMS: 900, TPS: 111.1, Terminal: true, Streaming: true,
		},
	}
	if err := plugin.ApplyVerdict(context.Background(), "auth-1", initial, "passive", ""); err != nil {
		t.Fatal(err)
	}
	before, ok := plugin.stateAccount("auth-1")
	if !ok || before.LastEvidence == nil {
		t.Fatalf("initial quality evidence missing: %#v", before)
	}
	if err := plugin.ApplyVerdict(context.Background(), "auth-1", QualityVerdict{Class: ClassIgnored, Reason: "unsuccessful"}, "passive", ""); err != nil {
		t.Fatal(err)
	}
	afterIgnored, _ := plugin.stateAccount("auth-1")
	if afterIgnored.LastAttempt == nil || afterIgnored.LastAttempt.Class != ClassIgnored {
		t.Fatalf("ignored attempt was not recorded: %#v", afterIgnored.LastAttempt)
	}
	if !reflect.DeepEqual(afterIgnored.LastEvidence, before.LastEvidence) || !reflect.DeepEqual(afterIgnored.Metrics, before.Metrics) {
		t.Fatalf("ignored attempt overwrote quality evidence: before=%#v after=%#v", before, afterIgnored)
	}
	if err := plugin.ApplyVerdict(context.Background(), "auth-1", QualityVerdict{Class: ClassProbeError, Reason: "probe transport error"}, "probe", ""); err != nil {
		t.Fatal(err)
	}
	afterError, _ := plugin.stateAccount("auth-1")
	if afterError.LastAttempt == nil || afterError.LastAttempt.Class != ClassProbeError {
		t.Fatalf("probe error attempt was not recorded: %#v", afterError.LastAttempt)
	}
	if !reflect.DeepEqual(afterError.LastEvidence, before.LastEvidence) || !reflect.DeepEqual(afterError.Metrics, before.Metrics) {
		t.Fatalf("probe error overwrote quality evidence: before=%#v after=%#v", before, afterError)
	}
	if afterError.LastReason != ClassMissingThinking {
		t.Fatalf("last quality reason changed to %q", afterError.LastReason)
	}
}

func TestPassiveUsageFailureFiltering(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)

	nonGrok, errMarshal := json.Marshal(pluginapi.UsageRecord{
		AuthID: "auth-1", RequestID: "non-grok", Provider: "openai", Model: "gpt-5", Failed: true,
	})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	plugin.HandleUsage(nonGrok)
	account, _ := plugin.stateAccount("auth-1")
	if account.LastAttempt != nil {
		t.Fatalf("non-Grok failure was recorded as an attempt: %#v", account.LastAttempt)
	}

	grokFailure, errMarshal := json.Marshal(pluginapi.UsageRecord{
		AuthID: "auth-1", RequestID: "grok-failure", Provider: "xai", Model: "grok-4.5", Failed: true,
	})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	plugin.HandleUsage(grokFailure)
	account, _ = plugin.stateAccount("auth-1")
	if account.LastAttempt == nil || account.LastAttempt.Class != ClassIgnored || account.LastAttempt.Reason != "unsuccessful" {
		t.Fatalf("Grok failure was not recorded as ignored: %#v", account.LastAttempt)
	}
	if account.ErrorStrikes != 0 || account.LastEvidence != nil {
		t.Fatalf("Grok failure changed quality evidence: %#v", account)
	}
}

func TestAccountViewExposesQualityEvidence(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, func(config *Config) { config.AutoAction = "observe" })
	verdict := QualityVerdict{
		Class:  ClassMissingThinking,
		Reason: "no real reasoning evidence",
		Signals: Signals{
			OutputTokens: 100, VisibleTokens: 100, ReasoningTokens: 20, EncryptedBytes: 200,
			EncryptedFloor: 256, FirstTokenMS: 100, DurationMS: 1000, GenerationMS: 900,
			TPS: 111.1, Terminal: true, Streaming: true, Protocol: qualityProtocolResponses,
		},
	}
	if err := plugin.ApplyVerdict(context.Background(), "auth-1", verdict, "passive", ""); err != nil {
		t.Fatal(err)
	}
	response := actionManagementForTest(t, plugin, "accounts.list", map[string]any{"page_size": 10})
	if response.StatusCode != 200 {
		t.Fatalf("accounts.list status = %d body=%s", response.StatusCode, response.Body)
	}
	var decoded struct {
		Data struct {
			Items []accountView `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Data.Items) != 1 {
		t.Fatalf("unexpected account count: %d", len(decoded.Data.Items))
	}
	item := decoded.Data.Items[0]
	if item.LastAttempt == nil || item.LastAttempt.Class != ClassMissingThinking {
		t.Fatalf("last attempt missing from account view: %#v", item.LastAttempt)
	}
	if item.Evidence == nil || item.Evidence.Class != ClassMissingThinking || item.Evidence.Protocol != qualityProtocolResponses {
		t.Fatalf("quality evidence missing from account view: %#v", item.Evidence)
	}
	if strings.TrimSpace(item.Evidence.Reason) == "" || item.Evidence.OutputTokens != 100 || item.Evidence.EncryptedFloor != 256 {
		t.Fatalf("quality evidence fields incomplete: %#v", item.Evidence)
	}
}

func TestHoldExpiredBurstRequiresReasoningBill(t *testing.T) {
	cipher := Signals{
		HoldExpired: true, FirstVisible: true, VisibleFlushMS: 1500, VisibleTokens: 4,
		EncryptedBytes: 300, EncryptedFloor: 256,
	}
	if qualityIsBurstDump(cipher, 8) {
		t.Fatalf("cipher floor alone must not satisfy hold-expired burst reasoning bill: %#v", cipher)
	}
	cipher.ReasoningTokens = 80
	if !qualityIsBurstDump(cipher, 8) {
		t.Fatalf("reasoning bill at hold expiry should trigger burst: %#v", cipher)
	}
}
