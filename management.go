package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type rpcManagementRequest struct {
	pluginapi.ManagementRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type managementResponse struct {
	StatusCode int         `json:"StatusCode"`
	Headers    http.Header `json:"Headers"`
	Body       []byte      `json:"Body"`
}

type actionRequest struct {
	Action string         `json:"action"`
	Params map[string]any `json:"params"`
}

type restoreBatchItem struct {
	AuthID      string `json:"auth_id"`
	StateBefore string `json:"state_before"`
	Status      string `json:"status"`
	Error       string `json:"error,omitempty"`
}

type restoreBatchResult struct {
	Items                []restoreBatchItem `json:"items"`
	Count                int                `json:"count"`
	Restored             int                `json:"restored"`
	ExternalLeftDisabled int                `json:"external_left_disabled"`
	Failed               int                `json:"failed"`
}

type probeBatchItem struct {
	AuthID      string `json:"auth_id"`
	StateBefore string `json:"state_before"`
	StateAfter  string `json:"state_after,omitempty"`
	Status      string `json:"status"`
	Error       string `json:"error,omitempty"`
}

type probeBatchResult struct {
	Items   []probeBatchItem `json:"items"`
	Count   int              `json:"count"`
	Probed  int              `json:"probed"`
	Skipped int              `json:"skipped"`
	Failed  int              `json:"failed"`
}

type nonHealthyCandidate struct {
	authID      string
	stateBefore string
}

type accountView struct {
	AuthID                string           `json:"auth_id"`
	AuthIDDisplay         string           `json:"auth_id_display"`
	Display               string           `json:"display"`
	DisplayFull           string           `json:"display_full"`
	Email                 string           `json:"email,omitempty"`
	Label                 string           `json:"label,omitempty"`
	State                 string           `json:"state"`
	LastReason            string           `json:"last_reason,omitempty"`
	LastAttempt           *AttemptEvidence `json:"last_attempt,omitempty"`
	Evidence              *QualityEvidence `json:"evidence,omitempty"`
	LastObservedAt        time.Time        `json:"last_observed_at,omitempty"`
	LastProbeAt           time.Time        `json:"last_probe_at,omitempty"`
	CooldownUntil         time.Time        `json:"cooldown_until,omitempty"`
	ProbeBackoffUntil     time.Time        `json:"probe_backoff_until,omitempty"`
	DisableKind           string           `json:"disable_kind,omitempty"`
	StrikeMissingThinking bool             `json:"strike_missing_thinking"`
	SoftStrikes           int              `json:"soft_strikes"`
	ErrorStrikes          int              `json:"error_strikes"`
	ThinkingStrikes       int              `json:"thinking_strikes"`
	ResinManaged          bool             `json:"resin_managed"`
	ResinAccount          string           `json:"resin_account,omitempty"`
	ResinGeneration       int64            `json:"resin_generation"`
	ProxyStatus           string           `json:"proxy_status"`
	Proxy                 string           `json:"proxy,omitempty"`
	TPS                   float64          `json:"tps"`
	TTFTMS                int64            `json:"ttft_ms"`
	OutputTokens          int64            `json:"output_tokens"`
	ReasoningTokens       int64            `json:"reasoning_tokens"`
	VisibleTokens         int64            `json:"visible_tokens"`
	EncryptedBytes        int64            `json:"encrypted_bytes"`
	HasAccessToken        bool             `json:"has_access_token"`
	HasRefreshToken       bool             `json:"has_refresh_token"`
}

func (p *Plugin) HandleManagement(request []byte) ([]byte, error) {
	var req rpcManagementRequest
	if len(request) > 0 {
		if errUnmarshal := json.Unmarshal(request, &req); errUnmarshal != nil {
			return okEnvelope(managementResponse{StatusCode: http.StatusBadRequest, Headers: jsonHeaders(), Body: jsonError("invalid_request", "decode management request")})
		}
	}
	if strings.EqualFold(req.Method, http.MethodGet) && strings.HasSuffix(strings.TrimRight(req.Path, "/"), "/dashboard") {
		return okEnvelope(managementResponse{StatusCode: http.StatusOK, Headers: htmlHeaders(), Body: []byte(dashboardHTML())})
	}
	if !strings.EqualFold(req.Method, http.MethodPost) {
		return okEnvelope(managementResponse{StatusCode: http.StatusMethodNotAllowed, Headers: jsonHeaders(), Body: jsonError("method_not_allowed", "only POST is accepted")})
	}
	var action actionRequest
	if len(req.Body) > 0 {
		if errUnmarshal := json.Unmarshal(req.Body, &action); errUnmarshal != nil {
			return okEnvelope(managementResponse{StatusCode: http.StatusBadRequest, Headers: jsonHeaders(), Body: jsonError("invalid_json", "action body must be valid JSON")})
		}
	}

	if action.Params == nil {
		action.Params = map[string]any{}
	}
	ctx := context.Background()
	data, status, errAction := p.handleAction(ctx, action)
	if errAction != nil {
		if status == 0 {
			status = http.StatusBadRequest
		}
		return okEnvelope(managementResponse{StatusCode: status, Headers: jsonHeaders(), Body: jsonError(actionErrorCode(status), sanitizeError(errAction))})
	}
	if status == 0 {
		status = http.StatusOK
	}
	return okEnvelope(managementResponse{StatusCode: status, Headers: jsonHeaders(), Body: jsonOK(data)})
}

func (p *Plugin) handleAction(ctx context.Context, action actionRequest) (any, int, error) {
	switch strings.ToLower(strings.TrimSpace(action.Action)) {
	case "overview":
		return p.actionOverview(ctx)
	case "accounts.list":
		return p.actionAccountsList(ctx, action.Params)
	case "accounts.get":
		return p.actionAccountGet(ctx, action.Params)
	case "accounts.probe":
		return p.actionProbe(ctx, action.Params)
	case "accounts.probe_non_healthy":
		return p.actionProbeNonHealthy(ctx, action.Params)
	case "accounts.cooldown":
		return p.actionCooldown(ctx, action.Params)
	case "accounts.disable":
		return p.actionDisable(ctx, action.Params)
	case "accounts.restore":
		return p.actionRestore(ctx, action.Params)
	case "accounts.restore_non_healthy":
		return p.actionRestoreNonHealthy(ctx, action.Params)
	case "resin.bind":
		return p.actionResinBind(ctx, action.Params)
	case "resin.bind_unbound":
		return p.actionResinBindUnbound(ctx, action.Params)
	case "resin.rotate":
		return p.actionResinRotate(ctx, action.Params)
	case "resin.clear":
		return p.actionResinClear(ctx, action.Params)
	case "resin.config.get":
		return p.actionResinConfigGet()
	case "resin.config.validate":
		return p.actionResinConfigValidate(ctx, action.Params)
	case "resin.config.save":
		return p.actionResinConfigSave(ctx, action.Params)
	case "policy.get":
		return p.actionPolicyGet()
	case "policy.update":
		return p.actionPolicyUpdate(action.Params)
	case "policy.reset":
		return p.actionPolicyReset()
	case "state_file.update":
		return p.actionStateFileUpdate(action.Params)
	case "events.list":
		return p.actionEventsList(action.Params)
	case "tasks.get":
		return p.actionTaskGet(action.Params)
	case "tasks.list":
		return p.actionTaskList(action.Params)
	default:
		return nil, http.StatusBadRequest, fmt.Errorf("unknown action")
	}
}

func (p *Plugin) actionOverview(ctx context.Context) (any, int, error) {
	_ = p.RefreshCatalog(ctx)
	state := p.stateStore().Snapshot()
	counts := map[string]int{}
	for _, account := range state.Accounts {
		if account == nil {
			continue
		}
		counts[account.State]++
	}
	resinBound := 0
	resinUnbound := 0
	resinConflicts := 0
	for _, account := range p.catalogSnapshot() {
		_, _, raw, errResolve := p.resolveAuth(ctx, account.ID)
		if errResolve != nil {
			continue
		}
		meta := guardMeta(raw)
		proxy := strings.TrimSpace(authRawString(raw, "proxy_url"))
		if meta.ResinAccount != "" {
			resinBound++
		} else if proxy != "" {
			resinConflicts++
		} else {
			resinUnbound++
		}
	}
	recentAnomalies := 0
	since := time.Now().UTC().Add(-24 * time.Hour)
	for _, event := range state.Events {
		if event.Time.After(since) && event.Class != "" {
			recentAnomalies++
		}
	}
	p.mu.RLock()
	enabled := p.cfg.Enabled
	policy := p.policy
	p.mu.RUnlock()
	stateFile := ""
	if store := p.stateStore(); store != nil {
		stateFile = store.Path()
	}
	return map[string]any{
		"enabled":              enabled,
		"updated_at":           state.UpdatedAt,
		"auth_count":           len(p.catalogSnapshot()),
		"states":               counts,
		"resin":                map[string]int{"bound": resinBound, "unbound": resinUnbound, "conflict": resinConflicts},
		"recent_anomalies_24h": recentAnomalies,
		"policy":               policy,
		"state_file":           stateFile,
	}, http.StatusOK, nil
}

func (p *Plugin) actionAccountsList(ctx context.Context, params map[string]any) (any, int, error) {
	_ = p.RefreshCatalog(ctx)
	state := p.stateStore().Snapshot()
	filterState := strings.ToLower(strings.TrimSpace(asString(params["state"])))
	query := strings.ToLower(strings.TrimSpace(asString(params["query"])))
	page := intFromParams(params, "page", 1)
	pageSize := intFromParams(params, "page_size", 50)
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 200 {
		pageSize = 50
	}
	filtered := make([]Account, 0, len(p.catalogSnapshot()))
	for _, account := range p.catalogSnapshot() {
		if filterState != "" && catalogAccountState(account, state) != filterState {
			continue
		}
		if query != "" && !catalogAccountMatches(account, query) {
			continue
		}
		filtered = append(filtered, account)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].ID < filtered[j].ID })
	start := (page - 1) * pageSize
	if start > len(filtered) {
		start = len(filtered)
	}
	end := start + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}
	views := make([]accountView, 0, end-start)
	for _, account := range filtered[start:end] {
		view, errView := p.accountView(ctx, account)
		if errView != nil {
			continue
		}
		views = append(views, view)
	}
	return map[string]any{
		"items":        views,
		"page":         page,
		"page_size":    pageSize,
		"total":        len(filtered),
		"state_counts": stateCounts(state),
	}, http.StatusOK, nil
}

func catalogAccountState(account Account, state State) string {
	if stateAccount := state.Accounts[account.ID]; stateAccount != nil && stateAccount.State != "" {
		return stateAccount.State
	}
	if account.Disabled {
		return StateExternallyDisabled
	}
	return StateUnknown
}

func catalogAccountMatches(account Account, query string) bool {
	text := strings.ToLower(strings.Join([]string{
		firstNonEmpty(account.Email, account.Label, account.ID),
		account.ID,
		account.Label,
		account.Email,
	}, " "))
	return strings.Contains(text, query)
}

func (p *Plugin) nonHealthyCandidates(ctx context.Context) []nonHealthyCandidate {
	_ = p.RefreshCatalog(ctx)
	state := p.stateStore().Snapshot()
	catalog := p.catalogSnapshot()
	candidates := make([]nonHealthyCandidate, 0, len(catalog))
	for _, account := range catalog {
		stateBefore := catalogAccountState(account, state)
		if stateBefore == StateHealthy {
			continue
		}
		candidates = append(candidates, nonHealthyCandidate{authID: account.ID, stateBefore: stateBefore})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].authID < candidates[j].authID })
	return candidates
}

func nonHealthyCandidateAuthIDs(candidates []nonHealthyCandidate) []string {
	authIDs := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		authIDs = append(authIDs, candidate.authID)
	}
	return authIDs
}

func (p *Plugin) actionAccountGet(ctx context.Context, params map[string]any) (any, int, error) {
	authID := strings.TrimSpace(asString(params["auth_id"]))
	if authID == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("auth_id is required")
	}
	account, okAccount := p.findAccount(authID)
	if !okAccount {
		return nil, http.StatusNotFound, fmt.Errorf("auth not found")
	}
	view, errView := p.accountView(ctx, account)
	if errView != nil {
		return nil, http.StatusBadGateway, errView
	}
	return view, http.StatusOK, nil
}

func (p *Plugin) actionProbe(ctx context.Context, params map[string]any) (any, int, error) {
	authID := strings.TrimSpace(asString(params["auth_id"]))
	if authID == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("auth_id is required")
	}
	if _, okAccount := p.findAccount(authID); !okAccount {
		_ = p.RefreshCatalog(ctx)
		if _, okAccount = p.findAccount(authID); !okAccount {
			return nil, http.StatusNotFound, fmt.Errorf("auth not found")
		}
	}
	force := boolFromParams(params, "force")
	task := p.startTask("accounts.probe", []string{authID}, "正在执行主动探测", func(taskCtx context.Context) (any, error) {
		// Detached tasks must not reuse the short-lived management callback
		if errProbe := p.ProbeAccount(taskCtx, authID, force, ""); errProbe != nil {
			return nil, errProbe
		}
		account, okAccount := p.findAccount(authID)
		if !okAccount {
			return nil, fmt.Errorf("auth not found")
		}
		return p.accountView(taskCtx, account)
	})
	return task, http.StatusAccepted, nil
}

func (p *Plugin) actionProbeNonHealthy(ctx context.Context, _ map[string]any) (any, int, error) {
	if p.hasActiveTask("accounts.probe_non_healthy", "") {
		return nil, http.StatusConflict, fmt.Errorf("probe task is already running")
	}
	candidates := p.nonHealthyCandidates(ctx)
	authIDs := nonHealthyCandidateAuthIDs(candidates)
	task := p.startTask("accounts.probe_non_healthy", authIDs, fmt.Sprintf("正在检测 %d 个非健康账号", len(candidates)), func(taskCtx context.Context) (any, error) {
		result := probeBatchResult{
			Items: make([]probeBatchItem, len(candidates)),
			Count: len(candidates),
		}
		var wg sync.WaitGroup
		for index, candidate := range candidates {
			result.Items[index] = probeBatchItem{AuthID: candidate.authID, StateBefore: candidate.stateBefore}
			if candidate.stateBefore == StateDisabledByGuard || candidate.stateBefore == StateExternallyDisabled {
				result.Items[index].Status = "skipped"
				continue
			}
			wg.Add(1)
			go func(index int, candidate nonHealthyCandidate) {
				defer wg.Done()
				entry := probeBatchItem{AuthID: candidate.authID, StateBefore: candidate.stateBefore}
				if errProbe := p.ProbeAccount(taskCtx, candidate.authID, true, ""); errProbe != nil {
					entry.Status = "failed"
					entry.Error = sanitizeError(errProbe)
					result.Items[index] = entry
					return
				}
				account, okAccount := p.findAccount(candidate.authID)
				if !okAccount {
					entry.Status = "failed"
					entry.Error = "auth not found"
					result.Items[index] = entry
					return
				}
				view, errView := p.accountView(taskCtx, account)
				if errView != nil {
					entry.Status = "failed"
					entry.Error = sanitizeError(errView)
					result.Items[index] = entry
					return
				}
				entry.StateAfter = view.State
				entry.Status = "probed"
				result.Items[index] = entry
			}(index, candidate)
		}
		wg.Wait()
		for _, item := range result.Items {
			switch item.Status {
			case "probed":
				result.Probed++
			case "skipped":
				result.Skipped++
			case "failed":
				result.Failed++
			}
		}
		return result, nil
	})
	return task, http.StatusAccepted, nil
}

func (p *Plugin) actionCooldown(ctx context.Context, params map[string]any) (any, int, error) {
	authID := strings.TrimSpace(asString(params["auth_id"]))
	if authID == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("auth_id is required")
	}
	duration := time.Duration(0)
	if raw := strings.TrimSpace(asString(params["duration"])); raw != "" {
		parsed, errParse := time.ParseDuration(raw)
		if errParse != nil || parsed <= 0 {
			return nil, http.StatusBadRequest, fmt.Errorf("duration must be a positive duration")
		}
		duration = parsed
	}
	if errCooldown := p.ManualCooldown(ctx, authID, duration); errCooldown != nil {
		return nil, http.StatusConflict, errCooldown
	}
	return p.actionAccountGet(ctx, map[string]any{"auth_id": authID})
}

func (p *Plugin) actionDisable(ctx context.Context, params map[string]any) (any, int, error) {
	authID := strings.TrimSpace(asString(params["auth_id"]))
	if authID == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("auth_id is required")
	}
	if errDisable := p.ManualDisable(ctx, authID, asString(params["reason"])); errDisable != nil {
		return nil, http.StatusConflict, errDisable
	}
	return p.actionAccountGet(ctx, map[string]any{"auth_id": authID})
}

func (p *Plugin) actionRestore(ctx context.Context, params map[string]any) (any, int, error) {
	authID := strings.TrimSpace(asString(params["auth_id"]))
	if authID == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("auth_id is required")
	}
	if errRestore := p.ManualRestore(ctx, authID); errRestore != nil {
		return nil, http.StatusConflict, errRestore
	}
	return p.actionAccountGet(ctx, map[string]any{"auth_id": authID})
}

func (p *Plugin) actionRestoreNonHealthy(ctx context.Context, _ map[string]any) (any, int, error) {
	if p.hasActiveTask("accounts.restore_non_healthy", "") {
		return nil, http.StatusConflict, fmt.Errorf("restore task is already running")
	}
	candidates := p.nonHealthyCandidates(ctx)
	authIDs := nonHealthyCandidateAuthIDs(candidates)
	task := p.startTask("accounts.restore_non_healthy", authIDs, fmt.Sprintf("正在恢复 %d 个非健康账号", len(candidates)), func(taskCtx context.Context) (any, error) {
		result := restoreBatchResult{
			Items: make([]restoreBatchItem, 0, len(candidates)),
			Count: len(candidates),
		}
		for _, item := range candidates {
			entry := restoreBatchItem{AuthID: item.authID, StateBefore: item.stateBefore}
			if errRestore := p.ManualRestore(taskCtx, item.authID); errRestore != nil {
				entry.Status = "failed"
				entry.Error = sanitizeError(errRestore)
				result.Failed++
			} else if item.stateBefore == StateExternallyDisabled {
				entry.Status = "external_left_disabled"
				result.ExternalLeftDisabled++
			} else {
				entry.Status = "restored"
				result.Restored++
			}
			result.Items = append(result.Items, entry)
		}
		return result, nil
	})
	return task, http.StatusAccepted, nil
}

func (p *Plugin) actionResinBind(_ context.Context, params map[string]any) (any, int, error) {
	authIDs := authIDsFromParams(params)
	if len(authIDs) == 0 {
		return nil, http.StatusBadRequest, fmt.Errorf("auth_id or auth_ids is required")
	}
	dryRun := boolFromParams(params, "dry_run")
	task := p.startTask("resin.bind", authIDs, "正在绑定 Resin 代理", func(taskCtx context.Context) (any, error) {
		results := make([]ResinBindResult, 0, len(authIDs))
		for _, authID := range authIDs {
			result, errBind := p.BindResin(taskCtx, authID, dryRun)
			if errBind != nil && result.Error == "" {
				result.Error = sanitizeError(errBind)
			}
			results = append(results, result)
		}
		return map[string]any{"items": results, "count": len(results)}, nil
	})
	return task, http.StatusAccepted, nil
}

func (p *Plugin) actionResinBindUnbound(_ context.Context, params map[string]any) (any, int, error) {
	dryRun := boolFromParams(params, "dry_run")
	task := p.startTask("resin.bind_unbound", nil, "正在批量绑定未绑定账号", func(taskCtx context.Context) (any, error) {
		results, errBind := p.BindUnboundResin(taskCtx, dryRun)
		if errBind != nil {
			return nil, errBind
		}
		return map[string]any{"items": results, "count": len(results)}, nil
	})
	return task, http.StatusAccepted, nil
}

func (p *Plugin) actionResinClear(_ context.Context, params map[string]any) (any, int, error) {
	authIDs := authIDsFromParams(params)
	if len(authIDs) == 0 {
		return nil, http.StatusBadRequest, fmt.Errorf("auth_id or auth_ids is required")
	}
	task := p.startTask("resin.clear", authIDs, "正在清除账号代理", func(taskCtx context.Context) (any, error) {
		results := make([]ResinClearResult, 0, len(authIDs))
		for _, authID := range authIDs {
			result, errClear := p.ClearResinProxy(taskCtx, authID)
			if errClear != nil && result.Error == "" {
				result.Error = sanitizeError(errClear)
			}
			results = append(results, result)
		}
		return map[string]any{"items": results, "count": len(results)}, nil
	})
	return task, http.StatusAccepted, nil
}

func (p *Plugin) actionResinRotate(_ context.Context, params map[string]any) (any, int, error) {
	authID := strings.TrimSpace(asString(params["auth_id"]))
	if authID == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("auth_id is required")
	}
	task := p.startTask("resin.rotate", []string{authID}, "正在轮换代理租约", func(taskCtx context.Context) (any, error) {
		if errRotate := p.RotateResin(taskCtx, authID); errRotate != nil {
			return nil, errRotate
		}
		account, okAccount := p.findAccount(authID)
		if !okAccount {
			return nil, fmt.Errorf("auth not found")
		}
		return p.accountView(taskCtx, account)
	})
	return task, http.StatusAccepted, nil
}

func (p *Plugin) actionPolicyGet() (any, int, error) {
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	return policy, http.StatusOK, nil
}

func (p *Plugin) actionPolicyUpdate(params map[string]any) (any, int, error) {
	rawPolicy, okPolicy := params["policy"]
	if !okPolicy {
		return nil, http.StatusBadRequest, fmt.Errorf("policy is required")
	}
	encoded, errMarshal := json.Marshal(rawPolicy)
	if errMarshal != nil {
		return nil, http.StatusBadRequest, errMarshal
	}
	var policy Policy
	if errUnmarshal := json.Unmarshal(encoded, &policy); errUnmarshal != nil {
		return nil, http.StatusBadRequest, errUnmarshal
	}
	normalized, errNormalize := normalizePolicy(policy)
	if errNormalize != nil {
		return nil, http.StatusBadRequest, errNormalize
	}
	if errStore := p.stateStore().Update(func(state *State) error {
		state.Policy = normalized
		if state.Resin.Configured {
			state.Resin.AutoRotateAfterQualityFailure = normalized.ResinAutoRotateAfterQualityFail
		}
		return nil
	}); errStore != nil {
		return nil, http.StatusInternalServerError, errStore
	}
	p.mu.Lock()
	p.policy = normalized
	p.cfg.TransparentGuard = normalized.TransparentGuard
	p.cfg.PassiveGuard = normalized.PassiveGuard
	p.cfg.AutoAction = normalized.AutoAction
	p.cfg.ModelPrefixes = append([]string(nil), normalized.ModelPrefixes...)
	p.cfg.Quality = normalized.Quality
	p.cfg.Probe = normalized.Probe
	p.cfg.Resin.AutoRotateAfterQualityFailure = normalized.ResinAutoRotateAfterQualityFail
	p.cfg.EventsLimit = normalized.EventsLimit
	p.mu.Unlock()
	p.wakeProbeWaiters()
	return normalized, http.StatusOK, nil
}

func (p *Plugin) actionPolicyReset() (any, int, error) {
	p.mu.RLock()
	reset := p.yamlPolicy
	revision := p.yamlRevision
	p.mu.RUnlock()
	normalized, errNormalize := normalizePolicy(reset)
	if errNormalize != nil {
		return nil, http.StatusInternalServerError, errNormalize
	}
	if errStore := p.stateStore().Update(func(state *State) error {
		state.Policy = normalized
		state.PolicyYAMLRevision = revision
		if state.Resin.Configured {
			state.Resin.AutoRotateAfterQualityFailure = normalized.ResinAutoRotateAfterQualityFail
		}
		return nil
	}); errStore != nil {
		return nil, http.StatusInternalServerError, errStore
	}
	p.mu.Lock()
	p.policy = normalized
	p.cfg.TransparentGuard = normalized.TransparentGuard
	p.cfg.PassiveGuard = normalized.PassiveGuard
	p.cfg.AutoAction = normalized.AutoAction
	p.cfg.ModelPrefixes = append([]string(nil), normalized.ModelPrefixes...)
	p.cfg.Quality = normalized.Quality
	p.cfg.Probe = normalized.Probe
	p.cfg.Resin.AutoRotateAfterQualityFailure = normalized.ResinAutoRotateAfterQualityFail
	p.cfg.EventsLimit = normalized.EventsLimit
	p.mu.Unlock()
	p.wakeProbeWaiters()
	return normalized, http.StatusOK, nil
}

func (p *Plugin) actionStateFileUpdate(params map[string]any) (any, int, error) {
	stateFile := strings.TrimSpace(asString(params["state_file"]))
	if stateFile == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("state_file is required")
	}
	activePath, errUpdate := p.UpdateStatePath(stateFile)
	if errUpdate != nil {
		switch {
		case errors.Is(errUpdate, os.ErrInvalid):
			return nil, http.StatusBadRequest, errUpdate
		case errors.Is(errUpdate, os.ErrExist):
			return nil, http.StatusConflict, errUpdate
		default:
			return nil, http.StatusInternalServerError, errUpdate
		}
	}
	return map[string]any{"state_file": activePath}, http.StatusOK, nil
}

func (p *Plugin) actionTaskGet(params map[string]any) (any, int, error) {
	taskID := strings.TrimSpace(asString(params["task_id"]))
	if taskID == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("task_id is required")
	}
	task, okTask := p.taskByID(taskID)
	if !okTask {
		return nil, http.StatusNotFound, fmt.Errorf("task not found")
	}
	return task, http.StatusOK, nil
}

func (p *Plugin) actionTaskList(params map[string]any) (any, int, error) {
	return map[string]any{"items": p.listTasks(intFromParams(params, "limit", 50))}, http.StatusOK, nil
}

func (p *Plugin) actionEventsList(params map[string]any) (any, int, error) {
	state := p.stateStore().Snapshot()
	events := append([]Event(nil), state.Events...)
	sort.Slice(events, func(i, j int) bool { return events[i].Time.After(events[j].Time) })
	authID := strings.TrimSpace(asString(params["auth_id"]))
	filtered := make([]Event, 0, len(events))
	for _, event := range events {
		if authID != "" && event.AuthID != authID {
			continue
		}
		filtered = append(filtered, event)
	}
	limit := intFromParams(params, "limit", 100)
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	cursor := intFromParams(params, "cursor", 0)
	if cursor < 0 {
		cursor = 0
	}
	start := cursor
	if start > len(filtered) {
		start = len(filtered)
	}
	end := start + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	return map[string]any{"items": filtered[start:end], "next_cursor": end, "total": len(filtered)}, http.StatusOK, nil
}

func (p *Plugin) accountView(ctx context.Context, account Account) (accountView, error) {
	_, _, raw, errResolve := p.resolveAuth(ctx, account.ID)
	if errResolve != nil {
		return accountView{}, errResolve
	}
	meta := guardMeta(raw)
	stateAccount, _ := p.stateAccount(account.ID)
	resinGeneration := stateAccount.Resin.Generation
	if meta.ResinGeneration > resinGeneration {
		resinGeneration = meta.ResinGeneration
	}
	displayFull := strings.TrimSpace(firstNonEmpty(account.Email, account.Label, account.ID))
	displayLabel := strings.TrimSpace(account.Label)
	if strings.Contains(displayLabel, "@") {
		displayLabel = maskEmail(displayLabel)
	}
	view := accountView{
		AuthID:                account.ID,
		AuthIDDisplay:         maskAuthID(account.ID),
		Display:               maskAccountIdentity(displayFull),
		DisplayFull:           displayFull,
		Email:                 maskEmail(account.Email),
		Label:                 displayLabel,
		State:                 stateAccount.State,
		LastReason:            stateAccount.LastReason,
		LastAttempt:           stateAccount.LastAttempt,
		Evidence:              stateAccount.LastEvidence,
		LastObservedAt:        stateAccount.LastObservedAt,
		LastProbeAt:           stateAccount.LastProbeAt,
		CooldownUntil:         stateAccount.CooldownUntil,
		ProbeBackoffUntil:     stateAccount.ProbeBackoffUntil,
		DisableKind:           stateAccount.DisableKind,
		StrikeMissingThinking: stateAccount.QualityStrikeMissingThinking,
		SoftStrikes:           stateAccount.SoftStrikes,
		ErrorStrikes:          stateAccount.ErrorStrikes,
		ThinkingStrikes:       stateAccount.ThinkingStrikes,
		ResinManaged:          meta.Managed || stateAccount.Resin.Managed,
		ResinAccount:          firstNonEmpty(meta.ResinAccount, stateAccount.Resin.Account),
		ResinGeneration:       resinGeneration,
		ProxyStatus:           "unbound",
		Proxy:                 maskProxyURL(authRawString(raw, "proxy_url")),
		TPS:                   stateAccount.Metrics.TPS,
		TTFTMS:                stateAccount.Metrics.FirstTokenMS,
		OutputTokens:          stateAccount.Metrics.OutputTokens,
		ReasoningTokens:       stateAccount.Metrics.ReasoningTokens,
		VisibleTokens:         stateAccount.Metrics.VisibleTokens,
		EncryptedBytes:        stateAccount.Metrics.EncryptedBytes,
		HasAccessToken:        strings.TrimSpace(authRawString(raw, "access_token")) != "",
		HasRefreshToken:       strings.TrimSpace(authRawString(raw, "refresh_token")) != "",
	}
	if view.State == "" {
		view.State = authDisabledState(raw)
		if view.State == "" {
			view.State = StateUnknown
		}
	}
	if view.ResinAccount != "" {
		view.ProxyStatus = "bound"
	} else if strings.TrimSpace(authRawString(raw, "proxy_url")) != "" {
		view.ProxyStatus = "conflict"
	}
	return view, nil
}

func stateCounts(state State) map[string]int {
	counts := map[string]int{}
	for _, account := range state.Accounts {
		if account != nil {
			counts[account.State]++
		}
	}
	return counts
}

func intFromParams(params map[string]any, key string, fallback int) int {
	if value, ok := numeric(params[key]); ok {
		return int(value)
	}
	return fallback
}

func authIDsFromParams(params map[string]any) []string {
	if rawIDs, ok := params["auth_ids"].([]any); ok {
		out := make([]string, 0, len(rawIDs))
		seen := make(map[string]struct{}, len(rawIDs))
		for _, rawID := range rawIDs {
			authID := strings.TrimSpace(asString(rawID))
			if authID == "" {
				continue
			}
			if _, exists := seen[authID]; exists {
				continue
			}
			seen[authID] = struct{}{}
			out = append(out, authID)
		}
		return out
	}
	authID := strings.TrimSpace(asString(params["auth_id"]))
	if authID == "" {
		return nil
	}
	return []string{authID}
}

func boolFromParams(params map[string]any, key string) bool {
	if value, ok := params[key].(bool); ok {
		return value
	}
	if text := strings.ToLower(strings.TrimSpace(asString(params[key]))); text != "" {
		return text == "1" || text == "true" || text == "yes"
	}
	return false
}

func actionErrorCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "invalid_request"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusTooManyRequests:
		return "rate_limited"
	case http.StatusBadGateway, http.StatusServiceUnavailable:
		return "upstream_error"
	default:
		return "internal_error"
	}
}

func jsonHeaders() http.Header {
	return http.Header{
		"Content-Type":  []string{"application/json; charset=utf-8"},
		"Cache-Control": []string{"no-store"},
	}
}

func htmlHeaders() http.Header {
	return http.Header{
		"Content-Type":  []string{"text/html; charset=utf-8"},
		"Cache-Control": []string{"no-store"},
	}
}
