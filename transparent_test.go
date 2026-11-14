package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func twoGrokAuths(t *testing.T) ([]pluginapi.HostAuthFileEntry, map[string]json.RawMessage) {
	t.Helper()
	entries1, auths1 := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	entries2, auths2 := singleGrokAuth(t, "auth-2", "bob@example.com", nil)
	entries1[0].Priority = 1
	entries2[0].Priority = 2
	auths := make(map[string]json.RawMessage, 2)
	for key, value := range auths1 {
		auths[key] = value
	}
	for key, value := range auths2 {
		auths[key] = value
	}
	return append(entries1, entries2...), auths
}

func badStreamScript() []pluginapi.HostModelStreamReadResponse {
	return []pluginapi.HostModelStreamReadResponse{
		streamData("data: {\"choices\":[{\"delta\":{\"content\":\"bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer \"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":100}}\n\n"),
		{Done: true},
	}
}

func goodStreamScript() []pluginapi.HostModelStreamReadResponse {
	return []pluginapi.HostModelStreamReadResponse{
		streamData("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"plan the answer\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{\"content\":\"good answer\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":40,\"completion_tokens_details\":{\"reasoning_tokens\":1}}}\n\n"),
		{Done: true},
	}
}

func streamRequestForTest() rpcExecutorRequest {
	body := []byte(`{"model":"grok-4.5","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	return rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{
			Model:           "grok-4.5",
			Format:          "codex",
			SourceFormat:    "codex",
			Stream:          true,
			OriginalRequest: body,
			Headers:         http.Header{"Content-Type": []string{"application/json"}},
		},
		HostCallbackID: "callback-1",
	}
}

func nonReasoningStreamRequestForTest() rpcExecutorRequest {
	body := []byte(`{"model":"grok-composer-2.5-fast","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	return rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{
			Model:           "grok-composer-2.5-fast",
			Format:          "codex",
			SourceFormat:    "codex",
			Stream:          true,
			OriginalRequest: body,
			Headers:         http.Header{"Content-Type": []string{"application/json"}},
		},
		HostCallbackID: "callback-non-reasoning",
	}
}

func enableTransparentForTest(cfg *Config) {
	cfg.TransparentGuard = true
	cfg.PassiveGuard = true
	cfg.Quality.SoftTPS = 1e9
	cfg.Quality.HardTPS = 2e9
}

func TestTransparentCandidatesIncludeProbeMarkedAccount(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	plugin.markProbeAuth("auth-1", time.Minute)

	candidates, errCandidates := plugin.transparentCandidates(context.Background())
	if errCandidates != nil {
		t.Fatal(errCandidates)
	}
	if len(candidates) != 1 || candidates[0].ID != "auth-1" {
		t.Fatalf("probe-marked account was excluded from transparent candidates: %#v", candidates)
	}
}

func TestTransparentCandidatesSkipQualitySuspectAccounts(t *testing.T) {
	for _, state := range []string{StateSuspect, StateDegraded} {
		t.Run(state, func(t *testing.T) {
			entries, auths := twoGrokAuths(t)
			host := newMockHostCall(entries, auths)
			plugin := newTestPlugin(t, host, enableTransparentForTest)
			if errUpdate := plugin.stateStore().Update(func(value *State) error {
				plugin.ensureStateAccount(value, "auth-1", "", "").State = state
				plugin.ensureStateAccount(value, "auth-2", "", "").State = StateHealthy
				return nil
			}); errUpdate != nil {
				t.Fatal(errUpdate)
			}
			plugin.markRecentSuccess("auth-1")

			candidates, errCandidates := plugin.transparentCandidates(context.Background())
			if errCandidates != nil {
				t.Fatal(errCandidates)
			}
			if len(candidates) != 1 || candidates[0].ID != "auth-2" {
				t.Fatalf("quality-suspect account remained eligible: %#v", candidates)
			}
		})
	}
}

func TestTransparentStreamFailsOverBeforeEmit(t *testing.T) {
	entries, auths := twoGrokAuths(t)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", badStreamScript()...)
	host.scriptStream("auth-2", goodStreamScript()...)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	plugin.runTransparentStreamBridge(context.Background(), streamRequestForTest(), "plugin-stream-1")

	emitted := host.emitted["plugin-stream-1"]
	var joined strings.Builder
	for _, payload := range emitted {
		joined.Write(payload)
	}
	if strings.Contains(joined.String(), "bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer ") {
		t.Fatalf("bad content leaked to client: %s", joined.String())
	}
	if !strings.Contains(joined.String(), "good answer") {
		t.Fatalf("good content was not delivered: %s", joined.String())
	}
	if detail := host.closes["plugin-stream-1"]; detail != nil {
		t.Fatalf("successful stream closed with error: %#v", detail)
	}
	badState, _ := plugin.stateAccount("auth-1")
	goodState, _ := plugin.stateAccount("auth-2")
	if badState.State != StateDegraded {
		t.Fatalf("bad auth state = %q, want degraded", badState.State)
	}
	if goodState.State != StateHealthy {
		t.Fatalf("good auth state = %q, want healthy", goodState.State)
	}
	if len(host.modelCalls) != 2 {
		t.Fatalf("transparent request used %d attempts, want 2", len(host.modelCalls))
	}
	if host.modelCalls[0].AuthID == host.modelCalls[1].AuthID {
		t.Fatalf("missing-thinking response was retried on the same account: %#v", host.modelCalls)
	}
}

func TestTransparentMissingThinkingSwitchesAccountBeforeProxyRotation(t *testing.T) {
	entries1, auths1 := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": "socks5h://Default.grok-alice-old1:proxy-secret@resin:2260",
	})
	entries2, auths2 := singleGrokAuth(t, "auth-2", "[EMAIL]", nil)
	entries1[0].Priority = 1
	entries2[0].Priority = 2
	auths := make(map[string]json.RawMessage, len(auths1)+len(auths2))
	for key, value := range auths1 {
		auths[key] = value
	}
	for key, value := range auths2 {
		auths[key] = value
	}
	host := newMockHostCall(append(entries1, entries2...), auths)
	host.scriptStreamSequence("auth-1", badStreamScript(), badStreamScript())
	host.scriptStream("auth-2", goodStreamScript()...)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		enableTransparentForTest(cfg)
		cfg.Resin.Enabled = true
		cfg.Resin.LocalOnly = true
		cfg.Resin.AutoRotateAfterQualityFailure = true
	})
	seedResinState(t, plugin, "grok-alice-old1", 0)

	plugin.runTransparentStreamBridge(context.Background(), streamRequestForTest(), "plugin-stream-account-switch")

	if len(host.modelCalls) != 2 {
		t.Fatalf("missing-thinking request used %d attempts, want account switch after first failure", len(host.modelCalls))
	}
	if host.modelCalls[0].AuthID == host.modelCalls[1].AuthID {
		t.Fatalf("missing-thinking request retried the same account before switching: %#v", host.modelCalls)
	}
	for _, event := range plugin.stateStore().Snapshot().Events {
		if event.Action == "resin_rotate" {
			t.Fatalf("missing-thinking request rotated the same account before switching: %#v", event)
		}
	}
}

func TestTransparentStreamFailsClosedWhenAllCandidatesAreQualitySuspect(t *testing.T) {
	entries, auths := twoGrokAuths(t)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	if errUpdate := plugin.stateStore().Update(func(value *State) error {
		plugin.ensureStateAccount(value, "auth-1", "", "").State = StateSuspect
		plugin.ensureStateAccount(value, "auth-2", "", "").State = StateDegraded
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}

	plugin.runTransparentStreamBridge(context.Background(), streamRequestForTest(), "plugin-stream-no-quality-candidates")

	if len(host.modelCalls) != 0 {
		t.Fatalf("quality-suspect accounts were reused: %#v", host.modelCalls)
	}
	detail := host.closes["plugin-stream-no-quality-candidates"]
	if detail == nil || detail.Code != "quality_degraded" || detail.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("unexpected close detail: %#v", detail)
	}
}

func TestTransparentStreamAllCandidatesExhausted(t *testing.T) {
	entries, auths := twoGrokAuths(t)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", badStreamScript()...)
	host.scriptStream("auth-2", badStreamScript()...)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	plugin.runTransparentStreamBridge(context.Background(), streamRequestForTest(), "plugin-stream-2")
	if len(host.emitted["plugin-stream-2"]) != 0 {
		t.Fatalf("bad content was emitted: %#v", host.emitted["plugin-stream-2"])
	}
	detail := host.closes["plugin-stream-2"]
	if detail == nil || detail.Code != "quality_degraded" || detail.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("unexpected close detail: %#v", detail)
	}
}

func TestTransparentStreamFailOpenEmitsLastBufferedResponse(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", badStreamScript()...)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		enableTransparentForTest(cfg)
		cfg.Quality.OnExhausted = "fail_open"
	})
	plugin.runTransparentStreamBridge(context.Background(), streamRequestForTest(), "plugin-stream-3")
	emitted := host.emitted["plugin-stream-3"]
	var joined strings.Builder
	for _, payload := range emitted {
		joined.Write(payload)
	}
	if !strings.Contains(joined.String(), "bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer ") {
		t.Fatalf("fail_open did not emit buffered response: %s", joined.String())
	}
	if detail := host.closes["plugin-stream-3"]; detail != nil {
		t.Fatalf("fail_open closed with error: %#v", detail)
	}
}

func TestTransparentStreamUnsafeRequestUsesSingleAttempt(t *testing.T) {
	entries, auths := twoGrokAuths(t)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", badStreamScript()...)
	host.scriptStream("auth-2", goodStreamScript()...)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	req := streamRequestForTest()
	req.OriginalRequest = []byte(`{"model":"grok-4.5","previous_response_id":"resp_1"}`)
	plugin.runTransparentStreamBridge(context.Background(), req, "plugin-stream-4")
	detail := host.closes["plugin-stream-4"]
	if detail == nil || detail.Code != "quality_degraded" || detail.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("unexpected close detail: %#v", detail)
	}
	if len(host.modelCalls) != 1 {
		t.Fatalf("unsafe request used %d attempts, want 1", len(host.modelCalls))
	}
	if len(host.emitted["plugin-stream-4"]) != 0 {
		t.Fatal("unsafe request emitted degraded content")
	}
}

func TestTransparentNonReasoningStreamDelivered(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1",
		streamData("data: {\"choices\":[{\"delta\":{\"content\":\"plain answer without any reasoning content at all\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":12}}\n\n"),
		pluginapi.HostModelStreamReadResponse{Done: true},
	)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	plugin.runTransparentStreamBridge(context.Background(), nonReasoningStreamRequestForTest(), "plugin-stream-non-reasoning")

	var joined strings.Builder
	for _, payload := range host.emitted["plugin-stream-non-reasoning"] {
		joined.Write(payload)
	}
	if !strings.Contains(joined.String(), "plain answer without any reasoning content at all") {
		t.Fatalf("non-reasoning stream was not delivered: %s", joined.String())
	}
	if detail := host.closes["plugin-stream-non-reasoning"]; detail != nil {
		t.Fatalf("non-reasoning stream closed with error: %#v", detail)
	}
	if len(host.modelCalls) != 1 {
		t.Fatalf("non-reasoning stream used %d attempts, want 1", len(host.modelCalls))
	}
}

func TestTransparentNonReasoningEmptyStreamRejected(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", pluginapi.HostModelStreamReadResponse{Done: true})
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	plugin.runTransparentStreamBridge(context.Background(), nonReasoningStreamRequestForTest(), "plugin-stream-non-reasoning-empty")

	if len(host.emitted["plugin-stream-non-reasoning-empty"]) != 0 {
		t.Fatalf("empty stream emitted content: %#v", host.emitted["plugin-stream-non-reasoning-empty"])
	}
	detail := host.closes["plugin-stream-non-reasoning-empty"]
	if detail == nil || detail.Code != "quality_degraded" || detail.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("unexpected close detail: %#v", detail)
	}
}

func TestTransparentStreamBufferOverflowRejectedBeforeEmit(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1",
		streamData("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"),
		pluginapi.HostModelStreamReadResponse{Done: true},
	)
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		enableTransparentForTest(cfg)
		cfg.Quality.HoldMaxBufferBytes = 1
	})
	plugin.runTransparentStreamBridge(context.Background(), nonReasoningStreamRequestForTest(), "plugin-stream-buffer-overflow")

	if len(host.emitted["plugin-stream-buffer-overflow"]) != 0 {
		t.Fatalf("buffer overflow emitted content: %s", host.emitted["plugin-stream-buffer-overflow"])
	}
	detail := host.closes["plugin-stream-buffer-overflow"]
	if detail == nil || detail.Code != "quality_degraded" || detail.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("unexpected close detail: %#v", detail)
	}
}

func TestTransparentNonStreamingReturns503WhenExhausted(t *testing.T) {
	entries, auths := twoGrokAuths(t)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	req := streamRequestForTest()
	req.Stream = false
	req.OriginalRequest = []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}]}`)
	encoded, errMarshal := json.Marshal(req)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	response, errExecute := plugin.Execute(encoded)
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	var envelope struct {
		OK    bool `json:"ok"`
		Error struct {
			Code       string `json:"code"`
			HTTPStatus int    `json:"http_status"`
		} `json:"error"`
	}
	if errUnmarshal := json.Unmarshal(response, &envelope); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if envelope.OK || envelope.Error.Code != "quality_degraded" || envelope.Error.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("unexpected executor response: %s", response)
	}
}
func TestRouteModelSafetyAndCandidateChecks(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	request := rpcModelRouteRequest{ModelRouteRequest: pluginapi.ModelRouteRequest{
		SourceFormat:   "codex",
		RequestedModel: "grok-4.5",
		Stream:         true,
		Body:           []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}]}`),
	}}
	encoded, errMarshal := json.Marshal(request)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	raw, errRoute := plugin.RouteModel(encoded)
	if errRoute != nil {
		t.Fatal(errRoute)
	}
	var routeEnvelope struct {
		OK     bool                         `json:"ok"`
		Result pluginapi.ModelRouteResponse `json:"result"`
	}
	if errUnmarshal := json.Unmarshal(raw, &routeEnvelope); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if !routeEnvelope.OK || !routeEnvelope.Result.Handled {
		t.Fatalf("route was not handled: %s", raw)
	}
	request.Body = []byte(`{"model":"grok-4.5","mcp_servers":[{"url":"https://example.com"}]}`)
	encoded, _ = json.Marshal(request)
	raw, _ = plugin.RouteModel(encoded)
	_ = json.Unmarshal(raw, &routeEnvelope)
	if !routeEnvelope.Result.Handled {
		t.Fatal("unsafe MCP request bypassed the transparent gate")
	}
}

func responsesRequestForTest(model string, stream bool) rpcExecutorRequest {
	streamText := "false"
	if stream {
		streamText = "true"
	}
	body := []byte(`{"model":"` + model + `","stream":` + streamText + `,"input":"hi","reasoning":{"effort":"medium"}}`)
	return rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{
			Model:           model,
			Format:          "openai-response",
			SourceFormat:    "openai-response",
			Stream:          stream,
			OriginalRequest: body,
			Headers:         http.Header{"Content-Type": []string{"application/json"}},
		},
		HostCallbackID: "callback-responses",
	}
}

func goodResponsesStreamScript() []pluginapi.HostModelStreamReadResponse {
	return []pluginapi.HostModelStreamReadResponse{
		streamData("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"grok-4.6\"}}\n\n"),
		streamData("data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\",\"summary\":[]}}\n\n"),
		streamData("data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"plan the answer\"}\n\n"),
		streamData("data: {\"type\":\"response.output_text.delta\",\"delta\":\"good answer\"}\n\n"),
		streamData("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"output_tokens\":20,\"output_tokens_details\":{\"reasoning_tokens\":8}}}}\n\n"),
		{Done: true},
	}
}

func badResponsesStreamScript() []pluginapi.HostModelStreamReadResponse {
	return []pluginapi.HostModelStreamReadResponse{
		streamData("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_bad\",\"model\":\"grok-4.6\"}}\n\n"),
		streamData("data: {\"type\":\"response.output_text.delta\",\"delta\":\"bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer bad answer \"}\n\n"),
		streamData("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_bad\",\"usage\":{\"output_tokens\":100,\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"),
		{Done: true},
	}
}

func TestTransparentCodexResponsesThinkingStreamDelivered(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", goodResponsesStreamScript()...)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	plugin.runTransparentStreamBridge(context.Background(), responsesRequestForTest("grok-4.6", true), "plugin-stream-responses-good")

	joined := strings.Builder{}
	for _, payload := range host.emitted["plugin-stream-responses-good"] {
		joined.Write(payload)
	}
	if !strings.Contains(joined.String(), "good answer") || !strings.Contains(joined.String(), "response.reasoning_summary_text.delta") {
		t.Fatalf("thinking Responses stream was not delivered intact: %s", joined.String())
	}
	if detail := host.closes["plugin-stream-responses-good"]; detail != nil {
		t.Fatalf("thinking Responses stream closed with error: %#v", detail)
	}
	if len(host.modelCalls) != 1 {
		t.Fatalf("thinking Responses stream used %d accounts, want 1", len(host.modelCalls))
	}
}

func TestTransparentCodexResponsesTerminalIgnoresTrailingReadError(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	script := goodResponsesStreamScript()
	script = script[:len(script)-1]
	script = append(script, pluginapi.HostModelStreamReadResponse{
		Error: "unexpected EOF after response.completed",
		Done:  true,
	})
	host.scriptStream("auth-1", script...)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	plugin.runTransparentStreamBridge(context.Background(), responsesRequestForTest("grok-4.6", true), "plugin-stream-responses-terminal-error")

	joined := strings.Builder{}
	for _, payload := range host.emitted["plugin-stream-responses-terminal-error"] {
		joined.Write(payload)
	}
	if !strings.Contains(joined.String(), "good answer") || !strings.Contains(joined.String(), "response.completed") {
		t.Fatalf("complete terminal response was not delivered: %s", joined.String())
	}
	if detail := host.closes["plugin-stream-responses-terminal-error"]; detail != nil {
		t.Fatalf("terminal response closed with trailing error: %#v", detail)
	}
	if len(host.modelCalls) != 1 {
		t.Fatalf("terminal response used %d attempts, want 1", len(host.modelCalls))
	}
}

func TestTransparentCodexResponsesHeldBufferSurvivesTrailingReadError(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1",
		streamData("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_short\",\"model\":\"grok-4.6\"}}\n\n"),
		streamData("data: {\"type\":\"response.output_text.delta\",\"delta\":\"short\"}\n\n"),
		streamData("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_short\",\"usage\":{\"output_tokens\":3,\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"),
		pluginapi.HostModelStreamReadResponse{Error: "unexpected EOF after response.completed", Done: true},
	)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	plugin.runTransparentStreamBridge(context.Background(), responsesRequestForTest("grok-4.6", true), "plugin-stream-responses-held-terminal-error")

	joined := strings.Builder{}
	for _, payload := range host.emitted["plugin-stream-responses-held-terminal-error"] {
		joined.Write(payload)
	}
	if !strings.Contains(joined.String(), "short") || !strings.Contains(joined.String(), "response.completed") {
		t.Fatalf("held terminal response was not delivered: %s", joined.String())
	}
	if detail := host.closes["plugin-stream-responses-held-terminal-error"]; detail != nil {
		t.Fatalf("held terminal response closed with trailing error: %#v", detail)
	}
	if len(host.modelCalls) != 1 {
		t.Fatalf("held terminal response used %d attempts, want 1", len(host.modelCalls))
	}
}

func TestTransparentCodexResponsesMissingThinkingFailsOver(t *testing.T) {
	entries, auths := twoGrokAuths(t)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", badResponsesStreamScript()...)
	host.scriptStream("auth-2", goodResponsesStreamScript()...)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	plugin.runTransparentStreamBridge(context.Background(), responsesRequestForTest("grok-4.6", true), "plugin-stream-responses-failover")

	joined := strings.Builder{}
	for _, payload := range host.emitted["plugin-stream-responses-failover"] {
		joined.Write(payload)
	}
	if strings.Contains(joined.String(), "bad answer") {
		t.Fatalf("missing-thinking Responses content leaked: %s", joined.String())
	}
	if !strings.Contains(joined.String(), "good answer") {
		t.Fatalf("good Responses account was not delivered: %s", joined.String())
	}
}

func TestTransparentNonStreamResponsesThinkingDelivered(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptModelResponse("auth-1", pluginapi.HostModelExecutionResponse{
		StatusCode: http.StatusOK,
		Headers:    http.Header{"Content-Type": []string{"application/json"}},
		Body:       []byte(`{"id":"resp_1","object":"response","model":"grok-4.6","output":[{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"plan the answer"}]},{"id":"msg_1","type":"message","content":[{"type":"output_text","text":"final answer"}]}],"usage":{"input_tokens":10,"output_tokens":20,"output_tokens_details":{"reasoning_tokens":8}}}`),
	})
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	encoded, errMarshal := json.Marshal(responsesRequestForTest("grok-4.6", false))
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	response, errExecute := plugin.Execute(encoded)
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	var envelope struct {
		OK     bool                       `json:"ok"`
		Result pluginapi.ExecutorResponse `json:"result"`
		Error  pluginabi.Error            `json:"error"`
	}
	if errUnmarshal := json.Unmarshal(response, &envelope); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if !envelope.OK || !strings.Contains(string(envelope.Result.Payload), "final answer") {
		t.Fatalf("non-stream Responses with thinking was rejected: %s", response)
	}
}

func TestRouteModelUsesBodyModelAndHandlesAllGrokRequests(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, enableTransparentForTest)

	route := func(model, body string) pluginapi.ModelRouteResponse {
		request := rpcModelRouteRequest{ModelRouteRequest: pluginapi.ModelRouteRequest{
			SourceFormat:   "openai-response",
			RequestedModel: model,
			Stream:         true,
			Body:           []byte(body),
		}}
		encoded, errMarshal := json.Marshal(request)
		if errMarshal != nil {
			t.Fatal(errMarshal)
		}
		raw, errRoute := plugin.RouteModel(encoded)
		if errRoute != nil {
			t.Fatal(errRoute)
		}
		var envelope struct {
			OK     bool                         `json:"ok"`
			Result pluginapi.ModelRouteResponse `json:"result"`
		}
		if errUnmarshal := json.Unmarshal(raw, &envelope); errUnmarshal != nil {
			t.Fatal(errUnmarshal)
		}
		return envelope.Result
	}

	if result := route("", `{"model":"grok-4.6","input":"hi","reasoning":{"effort":"medium"}}`); !result.Handled {
		t.Fatalf("body model fallback was not used: %#v", result)
	}
	if result := route("grok-composer-2.5-fast", `{"model":"grok-composer-2.5-fast","input":"hi"}`); !result.Handled {
		t.Fatalf("non-reasoning Grok model bypassed the transparent gate: %#v", result)
	}
	if result := route("grok-4.6", `{"model":"grok-4.6","input":"hi","reasoning_effort":"none"}`); !result.Handled {
		t.Fatalf("reasoning-disabled Grok request bypassed the transparent gate: %#v", result)
	}
	if result := route("claude-sonnet-4-5", `{"model":"claude-sonnet-4-5","input":"hi"}`); result.Handled {
		t.Fatalf("non-Grok request was routed: %#v", result)
	}
}

func TestRouteModelUsesXAIAuthModelAlias(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.authModels = []pluginapi.HostAuthModels{{
		ID:       "auth-1",
		Provider: "xai",
		Models:   []string{"vendor-thinking-pro"},
	}}
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	request := rpcModelRouteRequest{ModelRouteRequest: pluginapi.ModelRouteRequest{
		SourceFormat:   "openai-response",
		RequestedModel: "vendor-thinking-pro",
		Stream:         true,
		Body:           []byte(`{"model":"vendor-thinking-pro","input":"hi"}`),
	}}
	encoded, _ := json.Marshal(request)
	raw, errRoute := plugin.RouteModel(encoded)
	if errRoute != nil {
		t.Fatal(errRoute)
	}
	var envelope struct {
		OK     bool                         `json:"ok"`
		Result pluginapi.ModelRouteResponse `json:"result"`
	}
	if errUnmarshal := json.Unmarshal(raw, &envelope); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if !envelope.OK || !envelope.Result.Handled {
		t.Fatalf("xAI auth model alias was not routed: %s", raw)
	}
}

func TestRouteModelHomeMetadataDoesNotBypassGate(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	request := rpcModelRouteRequest{ModelRouteRequest: pluginapi.ModelRouteRequest{
		SourceFormat:   "openai-response",
		RequestedModel: "grok-4.6",
		Stream:         true,
		Body:           []byte(`{"model":"grok-4.6","input":"hi"}`),
		Metadata:       map[string]any{"home_mode": true, "home": true},
	}}
	encoded, _ := json.Marshal(request)
	raw, errRoute := plugin.RouteModel(encoded)
	if errRoute != nil {
		t.Fatal(errRoute)
	}
	var envelope struct {
		OK     bool                         `json:"ok"`
		Result pluginapi.ModelRouteResponse `json:"result"`
	}
	if errUnmarshal := json.Unmarshal(raw, &envelope); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if !envelope.OK || !envelope.Result.Handled {
		t.Fatalf("home metadata bypassed the transparent gate: %s", raw)
	}
}

func TestEnsureTransparentReasoningEvidenceInclude(t *testing.T) {
	body := []byte(`{"model":"grok-4.7","stream":true,"input":"hi","reasoning":{"effort":"xhigh"}}`)
	updated := ensureTransparentReasoningEvidence(body, "codex", "grok-4.7")
	var payload map[string]any
	if errUnmarshal := json.Unmarshal(updated, &payload); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	includes, _ := payload["include"].([]any)
	if len(includes) != 1 || includes[0] != "reasoning.encrypted_content" {
		t.Fatalf("include = %#v, want reasoning.encrypted_content", payload["include"])
	}

	withOther := []byte(`{"model":"grok-4.7","input":"hi","include":["no_inline_citations"],"seed":9007199254740993}`)
	updatedOther := ensureTransparentReasoningEvidence(withOther, "codex", "grok-4.7")
	if !strings.Contains(string(updatedOther), `"seed":9007199254740993`) {
		t.Fatalf("large integer was altered while adding include: %s", updatedOther)
	}
	if errUnmarshal := json.Unmarshal(updatedOther, &payload); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	includes, _ = payload["include"].([]any)
	if len(includes) != 2 || includes[0] != "no_inline_citations" || includes[1] != "reasoning.encrypted_content" {
		t.Fatalf("include = %#v, want existing include plus reasoning.encrypted_content", payload["include"])
	}

	existing := []byte(`{"model":"grok-4.7","input":"hi","include":["no_inline_citations","reasoning.encrypted_content"]}`)
	if got := ensureTransparentReasoningEvidence(existing, "openai-response", "grok-4.7"); string(got) != string(existing) {
		t.Fatalf("existing reasoning include was rewritten: %s", got)
	}

	disabled := []byte(`{"model":"grok-4.7","input":"hi","reasoning":{"effort":"none"}}`)
	if got := ensureTransparentReasoningEvidence(disabled, "codex", "grok-4.7"); string(got) != string(disabled) {
		t.Fatalf("reasoning-disabled request was rewritten: %s", got)
	}

	chat := []byte(`{"model":"grok-4.7","messages":[{"role":"user","content":"hi"}]}`)
	if got := ensureTransparentReasoningEvidence(chat, "openai", "grok-4.7"); string(got) != string(chat) {
		t.Fatalf("chat request was rewritten: %s", got)
	}
}

func TestTransparentStreamAddsReasoningIncludeBeforeHostExecution(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", goodResponsesStreamScript()...)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	request := responsesRequestForTest("grok-4.6", true)
	request.SourceFormat = "codex"
	request.Format = "codex"
	plugin.runTransparentStreamBridge(context.Background(), request, "plugin-stream-reasoning-include")

	if len(host.modelCalls) != 1 {
		t.Fatalf("request used %d attempts, want 1", len(host.modelCalls))
	}
	var payload map[string]any
	if errUnmarshal := json.Unmarshal(host.modelCalls[0].Body, &payload); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	includes, _ := payload["include"].([]any)
	found := false
	for _, value := range includes {
		if value == "reasoning.encrypted_content" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("host execution omitted reasoning.encrypted_content: %s", host.modelCalls[0].Body)
	}
}

func TestRouteModelBypassesResponsesCompaction(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	const compactionPrompt = "Your task is to produce a faithful summary. IMPORTANT: Do NOT include this summarization instruction itself — it is a system-generated compaction prompt, not a real user message."

	cases := []struct {
		name        string
		body        string
		wantHandled bool
	}{
		{name: "remote trigger", body: `{"model":"grok-4.7","input":[{"role":"user","content":"continue"},{"type":"compaction_trigger"}]}`},
		{name: "tui prompt", body: `{"model":"grok-4.7","input":[{"role":"user","content":"` + compactionPrompt + `"}]}`},
		{name: "normal text mention", body: `{"model":"grok-4.7","input":[{"role":"user","content":"explain compaction_trigger"}]}`, wantHandled: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := rpcModelRouteRequest{ModelRouteRequest: pluginapi.ModelRouteRequest{
				SourceFormat:   "openai-response",
				RequestedModel: "grok-4.7",
				Stream:         true,
				Body:           []byte(tc.body),
			}}
			encoded, errMarshal := json.Marshal(request)
			if errMarshal != nil {
				t.Fatal(errMarshal)
			}
			raw, errRoute := plugin.RouteModel(encoded)
			if errRoute != nil {
				t.Fatal(errRoute)
			}
			var envelope struct {
				OK     bool                         `json:"ok"`
				Result pluginapi.ModelRouteResponse `json:"result"`
			}
			if errUnmarshal := json.Unmarshal(raw, &envelope); errUnmarshal != nil {
				t.Fatal(errUnmarshal)
			}
			if envelope.Result.Handled != tc.wantHandled {
				t.Fatalf("handled = %t, want %t: %s", envelope.Result.Handled, tc.wantHandled, raw)
			}
			if !tc.wantHandled && envelope.Result.Reason != "responses_compaction" {
				t.Fatalf("compaction request missing bypass reason: %s", raw)
			}
		})
	}
}

func TestSSEOutputNormalizerAddsLineAndFrameBoundaries(t *testing.T) {
	normalizer := &sseOutputNormalizer{}
	chunks := [][]byte{
		[]byte("event: response.created"),
		[]byte(`data: {"type":"response.created","response":{"id":"resp_1"}}`),
		[]byte("event: response.output_text.delta"),
		[]byte(`data: {"type":"response.output_text.delta","delta":"good answer"}`),
	}
	var output []byte
	for _, chunk := range chunks {
		for _, payload := range normalizer.Feed(chunk) {
			output = append(output, payload...)
		}
	}
	for _, payload := range normalizer.Flush() {
		output = append(output, payload...)
	}

	want := "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n" +
		"event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"good answer\"}\n\n"
	if string(output) != want {
		t.Fatalf("unexpected normalized SSE output:\nGot:  %q\nWant: %q", output, want)
	}
	if strings.Contains(string(output), "createddata:") || strings.Contains(string(output), "deltadata:") {
		t.Fatalf("SSE field lines were concatenated: %q", output)
	}
}

func TestSSEOutputNormalizerBuffersSplitDataLine(t *testing.T) {
	normalizer := &sseOutputNormalizer{}
	var output []byte
	for _, chunk := range [][]byte{
		[]byte(`data: {"type":"response.created",`),
		[]byte(`"response":{"id":"resp_1"}}`),
	} {
		for _, payload := range normalizer.Feed(chunk) {
			output = append(output, payload...)
		}
	}
	for _, payload := range normalizer.Flush() {
		output = append(output, payload...)
	}
	want := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n"
	if string(output) != want {
		t.Fatalf("split data line was not framed: got %q want %q", output, want)
	}
}

func lineChunkedResponsesStreamScript() []pluginapi.HostModelStreamReadResponse {
	return []pluginapi.HostModelStreamReadResponse{
		streamData("event: response.created"),
		streamData(`data: {"type":"response.created","response":{"id":"resp_1","model":"grok-4.6"}}`),
		streamData("event: response.reasoning_summary_text.delta"),
		streamData(`data: {"type":"response.reasoning_summary_text.delta","delta":"plan the answer"}`),
		streamData("event: response.output_text.delta"),
		streamData(`data: {"type":"response.output_text.delta","delta":"good answer"}`),
		streamData("event: response.completed"),
		streamData(`data: {"type":"response.completed","response":{"id":"resp_1","usage":{"output_tokens":20,"output_tokens_details":{"reasoning_tokens":8}}}}`),
		{Done: true},
	}
}

func TestTransparentCodexResponsesLineChunkStreamDelivered(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", lineChunkedResponsesStreamScript()...)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	plugin.runTransparentStreamBridge(context.Background(), responsesRequestForTest("grok-4.6", true), "plugin-stream-responses-line-chunks")

	joined := strings.Builder{}
	for _, payload := range host.emitted["plugin-stream-responses-line-chunks"] {
		joined.Write(payload)
	}
	output := joined.String()
	for _, event := range []string{
		"response.created",
		"response.reasoning_summary_text.delta",
		"response.output_text.delta",
		"response.completed",
	} {
		if !strings.Contains(output, "event: "+event+"\ndata: ") {
			t.Fatalf("event %s lost its SSE line boundary: %s", event, output)
		}
	}
	if !strings.Contains(output, "good answer") || !strings.Contains(output, "response.completed") {
		t.Fatalf("line-chunked Responses stream was not delivered: %s", output)
	}
	for _, concatenated := range []string{"createddata:", "deltadata:", "completeddata:"} {
		if strings.Contains(output, concatenated) {
			t.Fatalf("SSE fields were concatenated as %q: %s", concatenated, output)
		}
	}
	if detail := host.closes["plugin-stream-responses-line-chunks"]; detail != nil {
		t.Fatalf("line-chunked Responses stream closed with error: %#v", detail)
	}
	if len(host.modelCalls) != 1 {
		t.Fatalf("line-chunked Responses stream used %d accounts, want 1", len(host.modelCalls))
	}
}

func TestExecuteStreamWithoutStreamIDFramesLineChunks(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", lineChunkedResponsesStreamScript()...)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	req := responsesRequestForTest("grok-4.6", true)
	req.StreamID = ""
	encoded, errMarshal := json.Marshal(req)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	raw, errExecute := plugin.ExecuteStream(encoded)
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	var envelope struct {
		OK     bool                   `json:"ok"`
		Result executorStreamResponse `json:"result"`
	}
	if errUnmarshal := json.Unmarshal(raw, &envelope); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if !envelope.OK {
		t.Fatalf("buffered stream failed: %s", raw)
	}
	joined := strings.Builder{}
	for _, chunk := range envelope.Result.Chunks {
		joined.Write(chunk.Payload)
	}
	output := joined.String()
	if !strings.Contains(output, "event: response.output_text.delta\ndata: ") ||
		!strings.Contains(output, "good answer") ||
		!strings.Contains(output, "response.completed") {
		t.Fatalf("buffered line chunks were not framed or delivered: %s", output)
	}
	for _, concatenated := range []string{"createddata:", "deltadata:", "completeddata:"} {
		if strings.Contains(output, concatenated) {
			t.Fatalf("buffered SSE fields were concatenated as %q: %s", concatenated, output)
		}
	}
}

func TestTransparentTPSIncludesPreStreamLatency(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", goodResponsesStreamScript()...)
	plugin := newTestPlugin(t, host, enableTransparentForTest)

	innerHostCall := hostCall
	hostCall = func(method string, payload any) (json.RawMessage, error) {
		switch method {
		case pluginabi.MethodHostModelExecuteStream:
			time.Sleep(200 * time.Millisecond)
		case pluginabi.MethodHostModelStreamRead:
			time.Sleep(10 * time.Millisecond)
		}
		return innerHostCall(method, payload)
	}
	plugin.runTransparentStreamBridge(context.Background(), responsesRequestForTest("grok-4.6", true), "plugin-stream-tps-clock")

	account, ok := plugin.stateAccount("auth-1")
	if !ok {
		t.Fatal("account state missing after transparent stream")
	}
	if account.Metrics.DurationMS < 180 || account.Metrics.FirstTokenMS < 180 {
		t.Fatalf("TPS clock started after the upstream wait: %#v", account.Metrics)
	}
	if account.Metrics.GenerationMS != account.Metrics.DurationMS {
		t.Fatalf("reasoning tail should use full request duration: %#v", account.Metrics)
	}
	if account.Metrics.TPS <= 0 || account.Metrics.TPS > 500 {
		t.Fatalf("transparent TPS = %.1f, want a sane grok2api-scale value: %#v", account.Metrics.TPS, account.Metrics)
	}
}

func TestTransparentUsesHostTranslatedExecutorPayload(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
	host := newMockHostCall(entries, auths)
	host.scriptStream("auth-1", lineChunkedResponsesStreamScript()...)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	req := responsesRequestForTest("grok-4.6", true)
	req.OriginalRequest = []byte(`{"model":"grok-4.6","messages":[{"role":"user","content":"original client format"}]}`)
	req.Payload = []byte(`{"model":"grok-4.6","stream":true,"input":"host translated payload"}`)
	req.Format = "codex"
	req.SourceFormat = "codex"
	plugin.runTransparentStreamBridge(context.Background(), req, "plugin-stream-translated-payload")

	if len(host.modelCalls) != 1 {
		t.Fatalf("transparent request used %d attempts, want 1", len(host.modelCalls))
	}
	execution := host.modelCalls[0]
	if !strings.Contains(string(execution.Body), "host translated payload") || strings.Contains(string(execution.Body), "original client format") {
		t.Fatalf("plugin used the original client body instead of the host-translated payload: %s", execution.Body)
	}
	if execution.EntryProtocol != "codex" || execution.ExitProtocol != "codex" {
		t.Fatalf("unexpected execution protocols: entry=%q exit=%q", execution.EntryProtocol, execution.ExitProtocol)
	}
}

func TestSSEOutputNormalizerPairsEventWithData(t *testing.T) {
	normalizer := &sseOutputNormalizer{}
	if output := normalizer.Feed([]byte("event: response.completed")); len(output) != 0 {
		t.Fatalf("event line emitted before its data line: %q", output)
	}
	output := normalizer.Feed([]byte(`data: {"type":"response.completed","response":{"id":"resp_1"}}`))
	want := "event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n"
	if len(output) != 1 || string(output[0]) != want {
		t.Fatalf("event and data were not emitted as one complete frame: %q", output)
	}
}
