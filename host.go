package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type authListResponse struct {
	Files []pluginapi.HostAuthFileEntry `json:"files"`
}

type hostModelExecutionRequest struct {
	pluginapi.HostModelExecutionRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

func (p *Plugin) listAuths() ([]pluginapi.HostAuthFileEntry, error) {
	result, errCall := hostCall(pluginabi.MethodHostAuthList, map[string]any{})
	if errCall != nil {
		return nil, errCall
	}
	var response authListResponse
	if errUnmarshal := json.Unmarshal(result, &response); errUnmarshal != nil {
		return nil, fmt.Errorf("decode host.auth.list result: %w", errUnmarshal)
	}
	return response.Files, nil
}

func (p *Plugin) getAuth(authIndex string) (pluginapi.HostAuthGetResponse, error) {
	result, errCall := hostCall(pluginabi.MethodHostAuthGet, pluginapi.HostAuthGetRequest{AuthIndex: authIndex})
	if errCall != nil {
		return pluginapi.HostAuthGetResponse{}, errCall
	}
	var response pluginapi.HostAuthGetResponse
	if errUnmarshal := json.Unmarshal(result, &response); errUnmarshal != nil {
		return pluginapi.HostAuthGetResponse{}, fmt.Errorf("decode host.auth.get result: %w", errUnmarshal)
	}
	return response, nil
}

func (p *Plugin) saveAuth(name string, raw json.RawMessage) (pluginapi.HostAuthSaveResponse, error) {
	result, errCall := hostCall(pluginabi.MethodHostAuthSave, pluginapi.HostAuthSaveRequest{Name: name, JSON: raw})
	if errCall != nil {
		return pluginapi.HostAuthSaveResponse{}, errCall
	}
	var response pluginapi.HostAuthSaveResponse
	if errUnmarshal := json.Unmarshal(result, &response); errUnmarshal != nil {
		return pluginapi.HostAuthSaveResponse{}, fmt.Errorf("decode host.auth.save result: %w", errUnmarshal)
	}
	return response, nil
}

func (p *Plugin) executeModel(_ context.Context, req pluginapi.HostModelExecutionRequest, callbackID string) (pluginapi.HostModelExecutionResponse, error) {
	result, errCall := hostCall(pluginabi.MethodHostModelExecute, hostModelExecutionRequest{HostModelExecutionRequest: req, HostCallbackID: callbackID})
	if errCall != nil {
		return pluginapi.HostModelExecutionResponse{}, errCall
	}
	var response pluginapi.HostModelExecutionResponse
	if errUnmarshal := json.Unmarshal(result, &response); errUnmarshal != nil {
		return pluginapi.HostModelExecutionResponse{}, fmt.Errorf("decode host.model.execute result: %w", errUnmarshal)
	}
	return response, nil
}

func (p *Plugin) executeModelStream(_ context.Context, req pluginapi.HostModelExecutionRequest, callbackID string) (pluginapi.HostModelStreamResponse, error) {
	result, errCall := hostCall(pluginabi.MethodHostModelExecuteStream, hostModelExecutionRequest{HostModelExecutionRequest: req, HostCallbackID: callbackID})
	if errCall != nil {
		return pluginapi.HostModelStreamResponse{}, errCall
	}
	var response pluginapi.HostModelStreamResponse
	if errUnmarshal := json.Unmarshal(result, &response); errUnmarshal != nil {
		return pluginapi.HostModelStreamResponse{}, fmt.Errorf("decode host.model.execute_stream result: %w", errUnmarshal)
	}
	if response.StreamID == "" {
		return pluginapi.HostModelStreamResponse{}, fmt.Errorf("host.model.execute_stream returned an empty stream_id")
	}
	p.trackModelStream(response.StreamID)
	return response, nil
}

func (p *Plugin) readModelStream(_ context.Context, streamID string) (pluginapi.HostModelStreamReadResponse, error) {
	result, errCall := hostCall(pluginabi.MethodHostModelStreamRead, pluginapi.HostModelStreamReadRequest{StreamID: streamID})
	if errCall != nil {
		return pluginapi.HostModelStreamReadResponse{}, errCall
	}
	var response pluginapi.HostModelStreamReadResponse
	if errUnmarshal := json.Unmarshal(result, &response); errUnmarshal != nil {
		return pluginapi.HostModelStreamReadResponse{}, fmt.Errorf("decode host.model.stream_read result: %w", errUnmarshal)
	}
	return response, nil
}

func (p *Plugin) closeModelStream(streamID string) error {
	if streamID == "" {
		return nil
	}
	_, errCall := hostCall(pluginabi.MethodHostModelStreamClose, pluginapi.HostModelStreamCloseRequest{StreamID: streamID})
	if errCall != nil {
		return errCall
	}
	p.untrackModelStream(streamID)
	return nil
}

func cloneHeader(headers http.Header) http.Header {
	if headers == nil {
		return nil
	}
	cloned := make(http.Header, len(headers))
	for key, values := range headers {
		cloned[key] = append([]string(nil), values...)
	}
	return cloned
}

func cloneValues(values url.Values) url.Values {
	if values == nil {
		return nil
	}
	cloned := make(url.Values, len(values))
	for key, items := range values {
		cloned[key] = append([]string(nil), items...)
	}
	return cloned
}
func rawAuthToMap(raw []byte) (map[string]json.RawMessage, error) {
	var value map[string]json.RawMessage
	if errUnmarshal := json.Unmarshal(raw, &value); errUnmarshal != nil {
		return nil, fmt.Errorf("decode auth JSON: %w", errUnmarshal)
	}
	if value == nil {
		value = make(map[string]json.RawMessage)
	}
	return value, nil
}

func marshalRawMap(value map[string]json.RawMessage) (json.RawMessage, error) {
	raw, errMarshal := json.Marshal(value)
	if errMarshal != nil {
		return nil, fmt.Errorf("encode auth JSON: %w", errMarshal)
	}
	return raw, nil
}

type modelStreamReadResult struct {
	Response pluginapi.HostModelStreamReadResponse
	Err      error
}

func (p *Plugin) readModelStreamContext(ctx context.Context, streamID string) (pluginapi.HostModelStreamReadResponse, error) {
	if ctx == nil || ctx.Done() == nil {
		return p.readModelStream(context.Background(), streamID)
	}
	resultChannel := make(chan modelStreamReadResult, 1)
	go func() {
		response, errRead := p.readModelStream(context.Background(), streamID)
		select {
		case resultChannel <- modelStreamReadResult{Response: response, Err: errRead}:
		case <-ctx.Done():
		}
	}()
	select {
	case <-ctx.Done():
		return pluginapi.HostModelStreamReadResponse{}, ctx.Err()
	case result := <-resultChannel:
		return result.Response, result.Err
	}
}
