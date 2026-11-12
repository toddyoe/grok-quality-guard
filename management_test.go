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
	headers := http.Header{"Content-Type": []string{"application/json"}}
	response := callManagementForTest(t, plugin, http.MethodPost, "/plugins/grok-quality-guard/api", headers, body)
	if response.StatusCode != http.StatusAccepted {
		return response
	}
	var envelope struct {
		Data Task `json:"data"`
	}
	if errUnmarshal := json.Unmarshal(response.Body, &envelope); errUnmarshal != nil {
		t.Fatalf("decode accepted task: %v", errUnmarshal)
	}
	task := waitTaskForTest(t, plugin, envelope.Data.ID)
	if task.Status == taskStatusFailed {
		status := task.ErrorStatus
		if status == 0 {
			status = http.StatusBadGateway
		}
		return managementResponse{StatusCode: status, Headers: jsonHeaders(), Body: jsonError(actionErrorCode(status), task.Error)}
	}
	return managementResponse{StatusCode: http.StatusOK, Headers: jsonHeaders(), Body: jsonOK(task.Result)}
}

func waitTaskForTest(t *testing.T, plugin *Plugin, taskID string) Task {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		task, okTask := plugin.taskByID(taskID)
		if !okTask {
			t.Fatalf("task %s disappeared", taskID)
		}
		if task.Status == taskStatusSucceeded || task.Status == taskStatusFailed {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not finish", taskID)
	return Task{}
}

func scriptHealthyProbeStream(t *testing.T, host *mockHostCall, authID string) {
	t.Helper()
	visible := strings.Repeat("reliable systems need careful design. ", 10) + "\\nQUALITY_OK"
	host.scriptStream(authID, []pluginapi.HostModelStreamReadResponse{
		streamData("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"plan carefully\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{\"content\":\"" + visible + "\"}}]}\n\n"),
		streamData("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":120,\"completion_tokens_details\":{\"reasoning_tokens\":1}}}\n\n"),
		{Done: true},
	}...)
}

func TestManagementResourceAndActionAccess(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	response := callManagementForTest(t, plugin, http.MethodGet, "/dashboard", nil, nil)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(response.Body), "/plugins/grok-quality-guard/api") {
		t.Fatalf("unexpected dashboard response: status=%d body=%s", response.StatusCode, response.Body)
	}
	for _, removed := range []string{"proxy_url", "代理（脱敏）", "选择全部筛选结果", " 代理", "代理 ", "const strikeBadge = strikes.length", "badge('连续 ' + strikes.join", "id=\"refreshBtn\""} {
		if strings.Contains(string(response.Body), removed) {
			t.Fatalf("dashboard still contains legacy label %q", removed)
		}
	}
	if strings.Contains(string(response.Body), "X-Grok-Quality-Guard-UI") {
		t.Fatal("dashboard still depends on the custom UI header")
	}
	if strings.Contains(string(response.Body), "对齐旧插件") {
		t.Fatal("dashboard should describe the setting semantically instead of referencing the old plugin")
	}
	for _, expected := range []string{"概览", "账号质量", "Resin 租约", "事件日志", "设置", "24 小时统计", "resin.config.get", "resin.clear", "leaseSelectAll", "clearSelectedBtn", "accountsPager", "leasesPager", "eventsPager", "selectFilteredBtn", "selectAllAccountsBtn", "全选当前筛选", "全选所有账号", "选择本页全部账号", "r_local_only", "本地轮换", "state_file.update", "id=\"p_state_file\" type=\"text\"", "summary-value truncate", "连续检测到缺少真实思考的响应达到该次数后触发处置或交叉验证", `type="password"`, "const REASON_DETAIL_LABELS", "'expected marker missing': '探测标记缺失'", "真实思考：", "终止：", "解析失败：", "<td class=\"nowrap\">", "actions-cell", "includeTime: false", "accountRefreshBtn", "leaseRefreshBtn", "eventRefreshBtn", "accountProbeAllBtn", "accounts.probe_non_healthy", "accountRestoreAllBtn", "accounts.restore_non_healthy"} {
		if !strings.Contains(string(response.Body), expected) {
			t.Fatalf("dashboard is missing %q", expected)
		}
	}
	body, _ := json.Marshal(actionRequest{Action: "overview"})
	response = callManagementForTest(t, plugin, http.MethodPost, "/plugins/grok-quality-guard/api", nil, body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("action without UI marker status = %d body=%s", response.StatusCode, response.Body)
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
	for _, forbidden := range []string{"proxy-secret", "access-auth-1", "refresh-auth-1"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("management response leaked %q: %s", forbidden, text)
		}
	}
	if !strings.Contains(text, "al***e@example.com") || !strings.Contains(text, "socks5h://example.com:1080") {
		t.Fatalf("management response did not include masked values: %s", text)
	}
	if !strings.Contains(text, "auth_id_display") {
		t.Fatalf("management response missing masked auth id: %s", text)
	}
	if !strings.Contains(text, "display_full") {
		t.Fatalf("management response missing full display value: %s", text)
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

func TestResinClearBatchManagement(t *testing.T) {
	entries1, auths1 := singleGrokAuth(t, "auth-1", "[EMAIL]", map[string]any{
		"proxy_url": "socks5h://user:secret-one@example.com:1080",
	})
	entries2, auths2 := singleGrokAuth(t, "auth-2", "[EMAIL]", map[string]any{
		"proxy_url": "socks5h://user:secret-two@example.org:1080",
	})
	entries := append(entries1, entries2...)
	auths := make(map[string]json.RawMessage, len(auths1)+len(auths2))
	for key, value := range auths1 {
		auths[key] = value
	}
	for key, value := range auths2 {
		auths[key] = value
	}
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	response := actionManagementForTest(t, plugin, "resin.clear", map[string]any{
		"auth_ids": []any{"auth-1", "auth-2", "auth-1"},
	})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.StatusCode, response.Body)
	}
	text := string(response.Body)
	if strings.Contains(text, "secret-one") || strings.Contains(text, "secret-two") {
		t.Fatalf("batch clear response leaked proxy credentials: %s", text)
	}
	for _, authID := range []string{"auth-1", "auth-2"} {
		saved, errDecode := rawAuthToMap(host.saves[authID])
		if errDecode != nil {
			t.Fatal(errDecode)
		}
		if authRawString(saved, "proxy_url") != "" {
			t.Fatalf("proxy_url for %s was not cleared", authID)
		}
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

func TestManualProbeRestoresGuardOwnedAccounts(t *testing.T) {
	cases := []struct {
		name   string
		action string
		params map[string]any
	}{
		{name: "cooling", action: "accounts.cooldown", params: map[string]any{"auth_id": "auth-1", "duration": "2h"}},
		{name: "disabled", action: "accounts.disable", params: map[string]any{"auth_id": "auth-1", "reason": "manual"}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			entries, auths := singleGrokAuth(t, "auth-1", "[EMAIL]", nil)
			host := newMockHostCall(entries, auths)
			scriptHealthyProbeStream(t, host, "auth-1")
			plugin := newTestPlugin(t, host, func(cfg *Config) {
				cfg.Quality.SoftTPS = 1e9
				cfg.Quality.HardTPS = 2e9
			})
			response := actionManagementForTest(t, plugin, testCase.action, testCase.params)
			if response.StatusCode != http.StatusOK {
				t.Fatalf("prepare guarded state status = %d body=%s", response.StatusCode, response.Body)
			}
			if account, _ := plugin.stateAccount("auth-1"); account.State == StateHealthy {
				t.Fatalf("account should be non-healthy before manual probe: %#v", account)
			}
			response = actionManagementForTest(t, plugin, "accounts.probe", map[string]any{"auth_id": "auth-1", "force": true})
			if response.StatusCode != http.StatusOK {
				t.Fatalf("manual probe status = %d body=%s", response.StatusCode, response.Body)
			}
			account, _ := plugin.stateAccount("auth-1")
			if account.State != StateHealthy {
				t.Fatalf("manual probe did not restore and probe account: %#v", account)
			}
			saved, errDecode := rawAuthToMap(host.saves["auth-1"])
			if errDecode != nil {
				t.Fatal(errDecode)
			}
			if authDisabled(saved) {
				t.Fatal("manual probe left auth disabled")
			}
		})
	}
}

func TestRestoreNonHealthyBatchManagement(t *testing.T) {
	entriesHealthy, authsHealthy := singleGrokAuth(t, "auth-healthy", "[EMAIL]", nil)
	entriesDegraded, authsDegraded := singleGrokAuth(t, "auth-degraded", "[EMAIL]", nil)
	entriesExternal, authsExternal := singleGrokAuth(t, "auth-external", "[EMAIL]", map[string]any{"disabled": true})
	entries := append(entriesHealthy, entriesDegraded...)
	entries = append(entries, entriesExternal...)
	auths := make(map[string]json.RawMessage, len(authsHealthy)+len(authsDegraded)+len(authsExternal))
	for key, value := range authsHealthy {
		auths[key] = value
	}
	for key, value := range authsDegraded {
		auths[key] = value
	}
	for key, value := range authsExternal {
		auths[key] = value
	}
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	if errUpdate := plugin.stateStore().Update(func(state *State) error {
		state.Accounts["auth-healthy"].State = StateHealthy
		state.Accounts["auth-degraded"].State = StateDegraded
		state.Accounts["auth-degraded"].CooldownUntil = time.Now().UTC().Add(time.Hour)
		state.Accounts["auth-external"].State = StateExternallyDisabled
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}

	body, errMarshal := json.Marshal(actionRequest{Action: "accounts.restore_non_healthy"})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	response := callManagementForTest(t, plugin, http.MethodPost, "/plugins/grok-quality-guard/api", http.Header{"Content-Type": []string{"application/json"}}, body)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("restore batch status = %d body=%s", response.StatusCode, response.Body)
	}
	var envelope struct {
		Data Task `json:"data"`
	}
	if errUnmarshal := json.Unmarshal(response.Body, &envelope); errUnmarshal != nil {
		t.Fatalf("decode restore task: %v", errUnmarshal)
	}
	if envelope.Data.Kind != "accounts.restore_non_healthy" {
		t.Fatalf("unexpected task kind: %q", envelope.Data.Kind)
	}
	task := waitTaskForTest(t, plugin, envelope.Data.ID)
	if task.Status != taskStatusSucceeded {
		t.Fatalf("restore batch task failed: %#v", task)
	}
	encodedResult, errMarshal := json.Marshal(task.Result)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	var result restoreBatchResult
	if errUnmarshal := json.Unmarshal(encodedResult, &result); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if result.Count != 2 || result.Restored != 1 || result.ExternalLeftDisabled != 1 || result.Failed != 0 {
		t.Fatalf("unexpected restore summary: %#v", result)
	}
	statuses := make(map[string]string, len(result.Items))
	for _, item := range result.Items {
		statuses[item.AuthID] = item.Status
	}
	if statuses["auth-degraded"] != "restored" || statuses["auth-external"] != "external_left_disabled" {
		t.Fatalf("unexpected restore items: %#v", result.Items)
	}
	state := plugin.stateStore().Snapshot()
	if state.Accounts["auth-healthy"].State != StateHealthy {
		t.Fatalf("healthy account was modified: %#v", state.Accounts["auth-healthy"])
	}
	if state.Accounts["auth-degraded"].State != StateUnknown || !state.Accounts["auth-degraded"].CooldownUntil.IsZero() {
		t.Fatalf("degraded account was not restored: %#v", state.Accounts["auth-degraded"])
	}
	if state.Accounts["auth-external"].State != StateUnknown {
		t.Fatalf("external account guard state was not cleared: %#v", state.Accounts["auth-external"])
	}
	if _, saved := host.saves["auth-external"]; saved {
		t.Fatal("external disabled auth must not be modified")
	}
}

func TestProbeNonHealthyBatchManagement(t *testing.T) {
	entriesHealthy, authsHealthy := singleGrokAuth(t, "auth-healthy", "[EMAIL]", nil)
	entriesSuspect, authsSuspect := singleGrokAuth(t, "auth-suspect", "[EMAIL]", nil)
	entriesGuard, authsGuard := singleGrokAuth(t, "auth-guard", "[EMAIL]", nil)
	entriesExternal, authsExternal := singleGrokAuth(t, "auth-external", "[EMAIL]", map[string]any{"disabled": true})
	entries := append(entriesHealthy, entriesSuspect...)
	entries = append(entries, entriesGuard...)
	entries = append(entries, entriesExternal...)
	auths := make(map[string]json.RawMessage, len(authsHealthy)+len(authsSuspect)+len(authsGuard)+len(authsExternal))
	for key, value := range authsHealthy {
		auths[key] = value
	}
	for key, value := range authsSuspect {
		auths[key] = value
	}
	for key, value := range authsGuard {
		auths[key] = value
	}
	for key, value := range authsExternal {
		auths[key] = value
	}
	host := newMockHostCall(entries, auths)
	scriptHealthyProbeStream(t, host, "auth-suspect")
	scriptHealthyProbeStream(t, host, "auth-guard")
	plugin := newTestPlugin(t, host, func(cfg *Config) {
		cfg.Quality.SoftTPS = 1e9
		cfg.Quality.HardTPS = 2e9
	})
	if errUpdate := plugin.stateStore().Update(func(state *State) error {
		state.Accounts["auth-healthy"].State = StateHealthy
		state.Accounts["auth-suspect"].State = StateSuspect
		state.Accounts["auth-external"].State = StateExternallyDisabled
		return nil
	}); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	if response := actionManagementForTest(t, plugin, "accounts.disable", map[string]any{"auth_id": "auth-guard", "reason": "manual"}); response.StatusCode != http.StatusOK {
		t.Fatalf("disable guard account status = %d body=%s", response.StatusCode, response.Body)
	}

	body, errMarshal := json.Marshal(actionRequest{Action: "accounts.probe_non_healthy"})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	response := callManagementForTest(t, plugin, http.MethodPost, "/plugins/grok-quality-guard/api", http.Header{"Content-Type": []string{"application/json"}}, body)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("probe batch status = %d body=%s", response.StatusCode, response.Body)
	}
	var envelope struct {
		Data Task `json:"data"`
	}
	if errUnmarshal := json.Unmarshal(response.Body, &envelope); errUnmarshal != nil {
		t.Fatalf("decode probe task: %v", errUnmarshal)
	}
	if envelope.Data.Kind != "accounts.probe_non_healthy" {
		t.Fatalf("unexpected task kind: %q", envelope.Data.Kind)
	}
	task := waitTaskForTest(t, plugin, envelope.Data.ID)
	if task.Status != taskStatusSucceeded {
		t.Fatalf("probe batch task failed: %#v", task)
	}
	encodedResult, errMarshal := json.Marshal(task.Result)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	var result probeBatchResult
	if errUnmarshal := json.Unmarshal(encodedResult, &result); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if result.Count != 3 || result.Probed != 2 || result.Skipped != 1 || result.Failed != 0 {
		t.Fatalf("unexpected probe summary: %#v", result)
	}
	statuses := make(map[string]string, len(result.Items))
	for _, item := range result.Items {
		statuses[item.AuthID] = item.Status
	}
	if statuses["auth-suspect"] != "probed" || statuses["auth-guard"] != "probed" || statuses["auth-external"] != "skipped" {
		t.Fatalf("unexpected probe items: %#v", result.Items)
	}
	state := plugin.stateStore().Snapshot()
	if state.Accounts["auth-healthy"].State != StateHealthy {
		t.Fatalf("healthy account was modified: %#v", state.Accounts["auth-healthy"])
	}
	if state.Accounts["auth-suspect"].State != StateHealthy {
		t.Fatalf("suspect account was not probed to healthy: %#v", state.Accounts["auth-suspect"])
	}
	if state.Accounts["auth-guard"].State != StateHealthy {
		t.Fatalf("guard-disabled account was not restored and probed: %#v", state.Accounts["auth-guard"])
	}
	if state.Accounts["auth-external"].State != StateExternallyDisabled {
		t.Fatalf("external account should be skipped: %#v", state.Accounts["auth-external"])
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

func TestManagementActionDoesNotRequireUIHeader(t *testing.T) {
	entries, auths := singleGrokAuth(t, "auth-1", "alice@example.com", nil)
	host := newMockHostCall(entries, auths)
	plugin := newTestPlugin(t, host, nil)
	body, errMarshal := json.Marshal(actionRequest{Action: "overview"})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	response := callManagementForTest(t, plugin, http.MethodPost, "/plugins/grok-quality-guard/api", http.Header{"Content-Type": []string{"application/json"}}, body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unmarked request status = %d body=%s", response.StatusCode, response.Body)
	}
}
