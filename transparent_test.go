package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

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
		streamData("data: {\"choices\":[{\"delta\":{\"content\":\"bad answer\"}}]}\n\n"),
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

func enableTransparentForTest(cfg *Config) {
	cfg.TransparentGuard = true
	cfg.PassiveGuard = true
	cfg.Quality.SoftTPS = 1e9
	cfg.Quality.HardTPS = 2e9
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
	if strings.Contains(joined.String(), "bad answer") {
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
	if !strings.Contains(joined.String(), "bad answer") {
		t.Fatalf("fail_open did not emit buffered response: %s", joined.String())
	}
	if detail := host.closes["plugin-stream-3"]; detail != nil {
		t.Fatalf("fail_open closed with error: %#v", detail)
	}
}

func TestTransparentStreamRejectsUnsafeRequest(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, enableTransparentForTest)
	req := streamRequestForTest()
	req.OriginalRequest = []byte(`{"model":"grok-4.5","previous_response_id":"resp_1"}`)
	plugin.runTransparentStreamBridge(context.Background(), req, "plugin-stream-4")
	detail := host.closes["plugin-stream-4"]
	if detail == nil || detail.Code != "request_not_safely_replayable" || detail.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("unexpected close detail: %#v", detail)
	}
	if len(host.modelCalls) != 0 {
		t.Fatal("unsafe request reached the host model executor")
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
	if routeEnvelope.Result.Handled {
		t.Fatal("unsafe MCP request was routed to the transparent executor")
	}
}
