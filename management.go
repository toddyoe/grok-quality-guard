package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
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

type accountView struct {
	AuthID                string    `json:"auth_id"`
	AuthIDDisplay         string    `json:"auth_id_display"`
	Display               string    `json:"display"`
	Email                 string    `json:"email,omitempty"`
	Label                 string    `json:"label,omitempty"`
	State                 string    `json:"state"`
	LastReason            string    `json:"last_reason,omitempty"`
	LastObservedAt        time.Time `json:"last_observed_at,omitempty"`
	LastProbeAt           time.Time `json:"last_probe_at,omitempty"`
	CooldownUntil         time.Time `json:"cooldown_until,omitempty"`
	ProbeBackoffUntil     time.Time `json:"probe_backoff_until,omitempty"`
	DisableKind           string    `json:"disable_kind,omitempty"`
	StrikeMissingThinking bool      `json:"strike_missing_thinking"`
	ResinManaged          bool      `json:"resin_managed"`
	ResinAccount          string    `json:"resin_account,omitempty"`
	ResinGeneration       int64     `json:"resin_generation"`
	ProxyStatus           string    `json:"proxy_status"`
	Proxy                 string    `json:"proxy,omitempty"`
	TPS                   float64   `json:"tps"`
	TTFTMS                int64     `json:"ttft_ms"`
	OutputTokens          int64     `json:"output_tokens"`
	ReasoningTokens       int64     `json:"reasoning_tokens"`
	VisibleTokens         int64     `json:"visible_tokens"`
	EncryptedBytes        int64     `json:"encrypted_bytes"`
	HasAccessToken        bool      `json:"has_access_token"`
	HasRefreshToken       bool      `json:"has_refresh_token"`
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
	data, status, errAction := p.handleAction(ctx, action, req.HostCallbackID)
	if errAction != nil {
		if status == 0 {
			status = http.StatusBadRequest
		}
		return okEnvelope(managementResponse{StatusCode: status, Headers: jsonHeaders(), Body: jsonError(actionErrorCode(status), sanitizeError(errAction))})
	}
	return okEnvelope(managementResponse{StatusCode: http.StatusOK, Headers: jsonHeaders(), Body: jsonOK(data)})
}

func (p *Plugin) handleAction(ctx context.Context, action actionRequest, callbackID string) (any, int, error) {
	switch strings.ToLower(strings.TrimSpace(action.Action)) {
	case "overview":
		return p.actionOverview(ctx)
	case "accounts.list":
		return p.actionAccountsList(ctx, action.Params)
	case "accounts.get":
		return p.actionAccountGet(ctx, action.Params)
	case "accounts.probe":
		return p.actionProbe(ctx, action.Params, callbackID)
	case "accounts.cooldown":
		return p.actionCooldown(ctx, action.Params)
	case "accounts.disable":
		return p.actionDisable(ctx, action.Params)
	case "accounts.restore":
		return p.actionRestore(ctx, action.Params)
	case "resin.bind":
		return p.actionResinBind(ctx, action.Params)
	case "resin.bind_unbound":
		return p.actionResinBindUnbound(ctx, action.Params)
	case "resin.rotate":
		return p.actionResinRotate(ctx, action.Params)
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
	case "events.list":
		return p.actionEventsList(action.Params)
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
	return map[string]any{
		"enabled":              enabled,
		"updated_at":           state.UpdatedAt,
		"auth_count":           len(p.catalogSnapshot()),
		"states":               counts,
		"resin":                map[string]int{"bound": resinBound, "unbound": resinUnbound, "conflict": resinConflicts},
		"recent_anomalies_24h": recentAnomalies,
		"policy":               policy,
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
	views := make([]accountView, 0)
	for _, account := range p.catalogSnapshot() {
		view, errView := p.accountView(ctx, account)
		if errView != nil {
			continue
		}
		if filterState != "" && strings.ToLower(view.State) != filterState {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(view.Display+" "+view.AuthID+" "+view.Label+" "+view.Email), query) {
			continue
		}
		views = append(views, view)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].AuthID < views[j].AuthID })
	start := (page - 1) * pageSize
	if start > len(views) {
		start = len(views)
	}
	end := start + pageSize
	if end > len(views) {
		end = len(views)
	}
	return map[string]any{
		"items":        views[start:end],
		"page":         page,
		"page_size":    pageSize,
		"total":        len(views),
		"state_counts": stateCounts(state),
	}, http.StatusOK, nil
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

func (p *Plugin) actionProbe(ctx context.Context, params map[string]any, callbackID string) (any, int, error) {
	authID := strings.TrimSpace(asString(params["auth_id"]))
	if authID == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("auth_id is required")
	}
	force := boolFromParams(params, "force")
	if errProbe := p.ProbeAccount(ctx, authID, force, callbackID); errProbe != nil {
		return nil, http.StatusBadGateway, errProbe
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

func (p *Plugin) actionResinBind(ctx context.Context, params map[string]any) (any, int, error) {
	dryRun := boolFromParams(params, "dry_run")
	if rawIDs, ok := params["auth_ids"].([]any); ok {
		results := make([]ResinBindResult, 0, len(rawIDs))
		for _, rawID := range rawIDs {
			authID := strings.TrimSpace(asString(rawID))
			if authID == "" {
				continue
			}
			result, errBind := p.BindResin(ctx, authID, dryRun)
			if errBind != nil && result.Error == "" {
				result.Error = errBind.Error()
			}
			results = append(results, result)
		}
		return results, http.StatusOK, nil
	}
	authID := strings.TrimSpace(asString(params["auth_id"]))
	if authID == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("auth_id or auth_ids is required")
	}
	result, errBind := p.BindResin(ctx, authID, dryRun)
	if errBind != nil {
		return result, http.StatusConflict, errBind
	}
	return result, http.StatusOK, nil
}

func (p *Plugin) actionResinBindUnbound(ctx context.Context, params map[string]any) (any, int, error) {
	results, errBind := p.BindUnboundResin(ctx, boolFromParams(params, "dry_run"))
	if errBind != nil {
		return nil, http.StatusBadGateway, errBind
	}
	return map[string]any{"items": results, "count": len(results)}, http.StatusOK, nil
}

func (p *Plugin) actionResinRotate(ctx context.Context, params map[string]any) (any, int, error) {
	authID := strings.TrimSpace(asString(params["auth_id"]))
	if authID == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("auth_id is required")
	}
	if errRotate := p.RotateResin(ctx, authID); errRotate != nil {
		return nil, http.StatusConflict, errRotate
	}
	return p.actionAccountGet(ctx, map[string]any{"auth_id": authID})
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
	return normalized, http.StatusOK, nil
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
	displayLabel := strings.TrimSpace(account.Label)
	if strings.Contains(displayLabel, "@") {
		displayLabel = maskEmail(displayLabel)
	}
	view := accountView{
		AuthID:                account.ID,
		AuthIDDisplay:         maskAuthID(account.ID),
		Display:               maskEmail(firstNonEmpty(account.Email, account.Label, account.ID)),
		Email:                 maskEmail(account.Email),
		Label:                 displayLabel,
		State:                 stateAccount.State,
		LastReason:            stateAccount.LastReason,
		LastObservedAt:        stateAccount.LastObservedAt,
		LastProbeAt:           stateAccount.LastProbeAt,
		CooldownUntil:         stateAccount.CooldownUntil,
		ProbeBackoffUntil:     stateAccount.ProbeBackoffUntil,
		DisableKind:           stateAccount.DisableKind,
		StrikeMissingThinking: stateAccount.QualityStrikeMissingThinking,
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
