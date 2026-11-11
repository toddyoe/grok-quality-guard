package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type managementEnvelopeForTest struct {
	OK     bool                `json:"ok"`
	Result managementResponse  `json:"result"`
	Error  *pluginapiErrorTest `json:"error"`
}

type pluginapiErrorTest struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func callManagementForTest(t *testing.T, plugin *Plugin, method, path string, headers http.Header, body []byte) managementResponse {
	t.Helper()
	request := rpcManagementRequest{
		ManagementRequest: pluginapi.ManagementRequest{
			Method:  method,
			Path:    path,
			Headers: headers,
			Body:    body,
		},
		HostCallbackID: "callback-1",
	}
	encoded, errMarshal := json.Marshal(request)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	rawResponse, errHandle := plugin.HandleManagement(encoded)
	if errHandle != nil {
		t.Fatal(errHandle)
	}
	var envelope managementEnvelopeForTest
	if errUnmarshal := json.Unmarshal(rawResponse, &envelope); errUnmarshal != nil {
		t.Fatalf("decode management envelope: %v", errUnmarshal)
	}
	if !envelope.OK {
		if envelope.Error != nil {
			t.Fatalf("management error: %s: %s", envelope.Error.Code, envelope.Error.Message)
		}
		t.Fatal("management request failed")
	}
	return envelope.Result
}

func actionManagementForTest(t *testing.T, plugin *Plugin, action string, params map[string]any) managementResponse {
	t.Helper()
	body, errMarshal := json.Marshal(actionRequest{Action: action, Params: params})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	headers := http.Header{"X-Grok-Quality-Guard-UI": []string{"1"}, "Content-Type": []string{"application/json"}}
	return callManagementForTest(t, plugin, http.MethodPost, "/plugins/grok-quality-guard/api", headers, body)
}

func TestManagementResourceAndHeaderChecks(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	response := callManagementForTest(t, plugin, http.MethodGet, "/dashboard", nil, nil)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(response.Body), "/plugins/grok-quality-guard/api") {
		t.Fatalf("unexpected dashboard response: status=%d body=%s", response.StatusCode, response.Body)
	}
	if !strings.Contains(string(response.Body), "resin.config.get") || !strings.Contains(string(response.Body), `type="password"`) {
		t.Fatalf("dashboard does not contain validated Resin configuration controls: %s", response.Body)
	}
	body, _ := json.Marshal(actionRequest{Action: "overview"})
	response = callManagementForTest(t, plugin, http.MethodPost, "/plugins/grok-quality-guard/api", nil, body)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("missing UI header status = %d, want 403", response.StatusCode)
	}
}

func TestAccountsListMasksSecrets(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", map[string]any{
		"proxy_url": "socks5h://user:proxy-secret@example.com:1080/private",
	})
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	response := actionManagementForTest(t, plugin, "accounts.list", map[string]any{"page_size": 10})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.StatusCode, response.Body)
	}
	text := string(response.Body)
	for _, forbidden := range []string{"proxy-secret", "access-auth-1", "refresh-auth-1", "alice@example.com"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("management response leaked %q: %s", forbidden, text)
		}
	}
	if !strings.Contains(text, "a***@example.com") || !strings.Contains(text, "socks5h://example.com:1080") {
		t.Fatalf("management response did not include masked values: %s", text)
	}
	if !strings.Contains(text, "auth_id_display") {
		t.Fatalf("management response missing masked auth id: %s", text)
	}
	var decoded struct {
		OK   bool `json:"ok"`
		Data struct {
			Items []accountView `json:"items"`
		} `json:"data"`
	}
	if errUnmarshal := json.Unmarshal(response.Body, &decoded); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if len(decoded.Data.Items) != 1 || decoded.Data.Items[0].AuthID != "auth-1" {
		t.Fatalf("unexpected accounts list: %#v", decoded.Data.Items)
	}
}

func TestPolicyUpdateAndReset(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	policy := PolicyFromConfig(plugin.cfg)
	policy.Quality.SoftTPS = 600
	policy.Quality.HardTPS = 1200
	response := actionManagementForTest(t, plugin, "policy.update", map[string]any{"policy": policy})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("policy update status = %d body=%s", response.StatusCode, response.Body)
	}
	if plugin.policy.Quality.SoftTPS != 600 || plugin.policy.Quality.HardTPS != 1200 {
		t.Fatalf("runtime policy not updated: %#v", plugin.policy.Quality)
	}
	state := plugin.store.Snapshot()
	if state.Policy.Quality.SoftTPS != 600 {
		t.Fatalf("persisted policy not updated: %#v", state.Policy.Quality)
	}
	response = actionManagementForTest(t, plugin, "policy.reset", nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("policy reset status = %d body=%s", response.StatusCode, response.Body)
	}
	if plugin.policy.Quality.SoftTPS != 500 || plugin.policy.Quality.HardTPS != 1000 {
		t.Fatalf("policy reset did not restore YAML defaults: %#v", plugin.policy.Quality)
	}
}

func TestInvalidPolicyUpdateIsRejected(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	policy := PolicyFromConfig(plugin.cfg)
	policy.Quality.SoftTPS = 1000
	policy.Quality.HardTPS = 500
	response := actionManagementForTest(t, plugin, "policy.update", map[string]any{"policy": policy})
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid policy status = %d body=%s", response.StatusCode, response.Body)
	}
}

func TestManualActionsUpdateState(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	response := actionManagementForTest(t, plugin, "accounts.cooldown", map[string]any{"auth_id": "auth-1", "duration": "2h"})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("cooldown status = %d body=%s", response.StatusCode, response.Body)
	}
	account, _ := plugin.stateAccount("auth-1")
	if account.State != StateCooling || time.Until(account.CooldownUntil) < 90*time.Minute {
		t.Fatalf("unexpected cooldown state: %#v", account)
	}
	response = actionManagementForTest(t, plugin, "accounts.restore", map[string]any{"auth_id": "auth-1"})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("restore status = %d body=%s", response.StatusCode, response.Body)
	}
	account, _ = plugin.stateAccount("auth-1")
	if account.State != StateUnknown || !account.CooldownUntil.IsZero() {
		t.Fatalf("unexpected restored state: %#v", account)
	}
}

func TestSanitizeErrorRedactsSecrets(t *testing.T) {
	input := errors.New(`Authorization: Bearer bearer-secret proxy=socks5h://user:proxy-pass@example.com:1080 "access_token":"access-secret" ?token=query-secret`)
	output := sanitizeError(input)
	for _, forbidden := range []string{"bearer-secret", "proxy-pass", "access-secret", "query-secret"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("sanitized error leaked %q: %s", forbidden, output)
		}
	}
	if !strings.Contains(output, "[redacted]") {
		t.Fatalf("sanitized error did not mark redactions: %s", output)
	}
}

func TestManagementUnknownActionReturnsBadRequest(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	response := actionManagementForTest(t, plugin, "does.not.exist", nil)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown action status = %d body=%s", response.StatusCode, response.Body)
	}
}
