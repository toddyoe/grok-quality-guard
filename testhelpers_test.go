package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

type mockHostCall struct {
	mu sync.Mutex

	entries []pluginapi.HostAuthFileEntry
	auths   map[string]json.RawMessage
	saves   map[string]json.RawMessage
	saveErr error

	nextStream int
	streams    map[string][]pluginapi.HostModelStreamReadResponse
	streamByID map[string]string
	emitted    map[string][][]byte
	closes     map[string]*pluginabi.Error
	modelCalls []pluginapi.HostModelExecutionRequest
	callbacks  []string
}

func newMockHostCall(entries []pluginapi.HostAuthFileEntry, auths map[string]json.RawMessage) *mockHostCall {
	return &mockHostCall{
		entries:    append([]pluginapi.HostAuthFileEntry(nil), entries...),
		auths:      cloneRawMap(auths),
		saves:      make(map[string]json.RawMessage),
		streams:    make(map[string][]pluginapi.HostModelStreamReadResponse),
		streamByID: make(map[string]string),
		emitted:    make(map[string][][]byte),
		closes:     make(map[string]*pluginabi.Error),
	}
}

func cloneRawMap(input map[string]json.RawMessage) map[string]json.RawMessage {
	output := make(map[string]json.RawMessage, len(input))
	for key, value := range input {
		output[key] = append(json.RawMessage(nil), value...)
	}
	return output
}

func (m *mockHostCall) scriptStream(authID string, reads ...pluginapi.HostModelStreamReadResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streams[authID] = append([]pluginapi.HostModelStreamReadResponse(nil), reads...)
}

func (m *mockHostCall) call(method string, payload any) (json.RawMessage, error) {
	encoded, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		return nil, errMarshal
	}
	switch method {
	case pluginabi.MethodHostAuthList:
		m.mu.Lock()
		defer m.mu.Unlock()
		return marshalTestResult(authListResponse{Files: append([]pluginapi.HostAuthFileEntry(nil), m.entries...)})
	case pluginabi.MethodHostAuthGet:
		var req pluginapi.HostAuthGetRequest
		if err := json.Unmarshal(encoded, &req); err != nil {
			return nil, err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		raw, ok := m.auths[req.AuthIndex]
		if !ok {
			return nil, fmt.Errorf("mock auth %s not found", req.AuthIndex)
		}
		return marshalTestResult(pluginapi.HostAuthGetResponse{AuthIndex: req.AuthIndex, Name: req.AuthIndex, JSON: append(json.RawMessage(nil), raw...)})
	case pluginabi.MethodHostAuthSave:
		var req pluginapi.HostAuthSaveRequest
		if err := json.Unmarshal(encoded, &req); err != nil {
			return nil, err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.saveErr != nil {
			return nil, m.saveErr
		}
		m.saves[req.Name] = append(json.RawMessage(nil), req.JSON...)
		m.auths[req.Name] = append(json.RawMessage(nil), req.JSON...)
		for i := range m.entries {
			if m.entries[i].Name != req.Name {
				continue
			}
			var decoded struct {
				Disabled bool   `json:"disabled"`
				Email    string `json:"email"`
			}
			_ = json.Unmarshal(req.JSON, &decoded)
			m.entries[i].Disabled = decoded.Disabled
			if decoded.Email != "" {
				m.entries[i].Email = decoded.Email
			}
		}
		return marshalTestResult(pluginapi.HostAuthSaveResponse{Name: req.Name, Path: req.Name})
	case pluginabi.MethodHostModelExecute:
		var req hostModelExecutionRequest
		if err := json.Unmarshal(encoded, &req); err != nil {
			return nil, err
		}
		m.mu.Lock()
		m.modelCalls = append(m.modelCalls, req.HostModelExecutionRequest)
		m.callbacks = append(m.callbacks, req.HostCallbackID)
		m.mu.Unlock()
		return marshalTestResult(pluginapi.HostModelExecutionResponse{StatusCode: http.StatusOK, Headers: http.Header{"Content-Type": []string{"application/json"}}, Body: []byte(`{"output_text":"ok"}`)})
	case pluginabi.MethodHostModelExecuteStream:
		var req hostModelExecutionRequest
		if err := json.Unmarshal(encoded, &req); err != nil {
			return nil, err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		m.modelCalls = append(m.modelCalls, req.HostModelExecutionRequest)
		m.callbacks = append(m.callbacks, req.HostCallbackID)
		reads, ok := m.streams[req.AuthID]
		if !ok {
			return nil, fmt.Errorf("mock stream for auth %s not configured", req.AuthID)
		}
		m.nextStream++
		streamID := fmt.Sprintf("stream-%d", m.nextStream)
		m.streamByID[streamID] = req.AuthID
		m.streams[req.AuthID] = append([]pluginapi.HostModelStreamReadResponse(nil), reads...)
		return marshalTestResult(pluginapi.HostModelStreamResponse{StatusCode: http.StatusOK, Headers: http.Header{"Content-Type": []string{"text/event-stream"}}, StreamID: streamID})
	case pluginabi.MethodHostModelStreamRead:
		var req pluginapi.HostModelStreamReadRequest
		if err := json.Unmarshal(encoded, &req); err != nil {
			return nil, err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		authID := m.streamByID[req.StreamID]
		reads := m.streams[authID]
		if len(reads) == 0 {
			return marshalTestResult(pluginapi.HostModelStreamReadResponse{Done: true})
		}
		next := reads[0]
		m.streams[authID] = reads[1:]
		return marshalTestResult(next)
	case pluginabi.MethodHostModelStreamClose:
		var req pluginapi.HostModelStreamCloseRequest
		if err := json.Unmarshal(encoded, &req); err != nil {
			return nil, err
		}
		m.mu.Lock()
		delete(m.streamByID, req.StreamID)
		m.mu.Unlock()
		return marshalTestResult(map[string]any{})
	case pluginabi.MethodHostStreamEmit:
		var req rpcStreamEmitRequest
		if err := json.Unmarshal(encoded, &req); err != nil {
			return nil, err
		}
		m.mu.Lock()
		m.emitted[req.StreamID] = append(m.emitted[req.StreamID], append([]byte(nil), req.Payload...))
		m.mu.Unlock()
		return marshalTestResult(map[string]any{})
	case pluginabi.MethodHostStreamClose:
		var req rpcStreamCloseRequest
		if err := json.Unmarshal(encoded, &req); err != nil {
			return nil, err
		}
		m.mu.Lock()
		m.closes[req.StreamID] = req.ErrorDetail
		m.mu.Unlock()
		return marshalTestResult(map[string]any{})
	default:
		return nil, fmt.Errorf("unexpected host method %s", method)
	}
}

func marshalTestResult(value any) (json.RawMessage, error) {
	encoded, errMarshal := json.Marshal(value)
	if errMarshal != nil {
		return nil, errMarshal
	}
	return json.RawMessage(encoded), nil
}

func makeAuthJSON(t *testing.T, values map[string]any) json.RawMessage {
	t.Helper()
	encoded, errMarshal := json.Marshal(values)
	if errMarshal != nil {
		t.Fatalf("marshal auth: %v", errMarshal)
	}
	return encoded
}

func newTestPlugin(t *testing.T, host *mockHostCall, mutate func(*Config)) *Plugin {
	t.Helper()
	originalHostCall := hostCall
	hostCall = host.call
	t.Cleanup(func() { hostCall = originalHostCall })

	cfg := DefaultConfig()
	cfg.Enabled = false
	cfg.StateFile = filepath.Join(t.TempDir(), "state", "state.json")
	if mutate != nil {
		mutate(&cfg)
	}
	rawConfig, errMarshal := yaml.Marshal(cfg)
	if errMarshal != nil {
		t.Fatalf("marshal config: %v", errMarshal)
	}
	plugin := NewPlugin()
	if errConfigure := plugin.Configure(rawConfig); errConfigure != nil {
		t.Fatalf("configure plugin: %v", errConfigure)
	}
	t.Cleanup(plugin.Shutdown)
	if errRefresh := plugin.RefreshCatalog(context.Background()); errRefresh != nil {
		t.Fatalf("refresh catalog: %v", errRefresh)
	}
	return plugin
}

func singleGrokAuth(t *testing.T, authID, email string, extra map[string]any) ([]pluginapi.HostAuthFileEntry, map[string]json.RawMessage) {
	t.Helper()
	values := map[string]any{
		"type":          "xai",
		"email":         email,
		"access_token":  "access-" + authID,
		"refresh_token": "refresh-" + authID,
		"disabled":      false,
	}
	for key, value := range extra {
		values[key] = value
	}
	entry := pluginapi.HostAuthFileEntry{
		ID:        authID,
		AuthIndex: authID,
		Name:      authID,
		Type:      "xai",
		Provider:  "xai",
		Label:     email,
		Email:     email,
		Priority:  1,
	}
	if disabled, ok := extra["disabled"].(bool); ok {
		entry.Disabled = disabled
	}
	return []pluginapi.HostAuthFileEntry{entry}, map[string]json.RawMessage{authID: makeAuthJSON(t, values)}
}

func streamData(payload string) pluginapi.HostModelStreamReadResponse {
	return pluginapi.HostModelStreamReadResponse{Payload: []byte(payload)}
}
