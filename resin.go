package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"

	"regexp"
	"strings"
	"time"
)

var resinAccountChars = regexp.MustCompile(`[^a-z0-9_-]+`)

const base36Alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"

type resinDeleteError struct {
	Status int
	Body   string
}

func (e *resinDeleteError) Error() string {
	return fmt.Sprintf("resin lease delete failed with status %d", e.Status)
}

type ResinClearResult struct {
	AuthID                  string `json:"auth_id"`
	Account                 string `json:"account,omitempty"`
	ProxyURL                string `json:"proxy_url,omitempty"`
	Cleared                 bool   `json:"cleared"`
	RemoteLeaseDeleteQueued bool   `json:"remote_lease_delete_queued,omitempty"`
	Skipped                 bool   `json:"skipped,omitempty"`
	Error                   string `json:"error,omitempty"`
}

type ResinBindResult struct {
	AuthID   string `json:"auth_id"`
	Account  string `json:"account"`
	ProxyURL string `json:"proxy_url,omitempty"`
	DryRun   bool   `json:"dry_run"`
	Conflict bool   `json:"conflict,omitempty"`
	Error    string `json:"error,omitempty"`
}

func (p *Plugin) BindResin(ctx context.Context, authID string, dryRun bool) (ResinBindResult, error) {
	lock := p.accountLock(authID)
	lock.Lock()
	defer lock.Unlock()

	cfg := p.ResinConfig()
	if !cfg.Enabled {
		return ResinBindResult{AuthID: authID, DryRun: dryRun, Error: "resin is disabled"}, fmt.Errorf("resin is disabled")
	}
	if cfg.LocalOnly {
		return ResinBindResult{AuthID: authID, DryRun: dryRun, Error: "local rotation does not create new proxy bindings"}, fmt.Errorf("resin API mode is required to bind new proxies")
	}
	_, response, raw, errResolve := p.resolveAuth(ctx, authID)
	if errResolve != nil {
		return ResinBindResult{AuthID: authID, DryRun: dryRun, Error: errResolve.Error()}, errResolve
	}
	stateAccount, _ := p.stateAccount(authID)
	existingProxy := strings.TrimSpace(authRawString(raw, "proxy_url"))
	boundAccount, managed := managedResinBinding(raw, stateAccount, cfg)
	if existingProxy != "" && !managed {
		return ResinBindResult{AuthID: authID, DryRun: dryRun, Conflict: true, Error: "auth already has a non-managed proxy_url"}, fmt.Errorf("proxy_url conflict")
	}
	if managed && boundAccount != "" && existingProxy != "" {
		return ResinBindResult{AuthID: authID, Account: boundAccount, ProxyURL: maskProxyURL(existingProxy), DryRun: dryRun}, nil
	}
	generated, errGenerate := p.generateResinAccount(authID, raw)
	if errGenerate != nil {
		return ResinBindResult{AuthID: authID, DryRun: dryRun, Error: errGenerate.Error()}, errGenerate
	}
	operationCfg, errResolvePlatform := resolveResinPlatformForOperation(ctx, cfg)
	if errResolvePlatform != nil {
		return ResinBindResult{AuthID: authID, DryRun: dryRun, Error: errResolvePlatform.Error()}, errResolvePlatform
	}
	proxyURL, errRender := renderResinProxy(operationCfg, generated)
	if errRender != nil {
		return ResinBindResult{AuthID: authID, DryRun: dryRun, Error: errRender.Error()}, errRender
	}
	result := ResinBindResult{AuthID: authID, Account: generated, ProxyURL: maskProxyURL(proxyURL), DryRun: dryRun}
	if dryRun {
		return result, nil
	}
	generation := stateAccount.Resin.Generation + 1
	setRawString(raw, "proxy_url", proxyURL)
	encoded, errMarshal := marshalRawMap(raw)
	if errMarshal != nil {
		return result, errMarshal
	}
	if _, errSave := p.saveAuth(response.Name, encoded); errSave != nil {
		return result, errSave
	}
	if errUpdate := p.updateStateAccount(authID, func(stateAccount *AccountState) error {
		stateAccount.Resin = ResinState{Managed: true, Account: generated, Generation: generation}
		if stateAccount.State == "" {
			stateAccount.State = StateUnknown
		}
		return nil
	}); errUpdate != nil {
		return result, errUpdate
	}
	_ = p.stateStore().Update(func(state *State) error {
		p.recordEvent(state, authID, "", "resin_bind", "bound", "account="+generated)
		return nil
	})
	return result, nil
}

func (p *Plugin) ClearResinProxy(ctx context.Context, authID string) (ResinClearResult, error) {
	lock := p.accountLock(authID)
	lock.Lock()
	defer lock.Unlock()

	_, response, raw, errResolve := p.resolveAuth(ctx, authID)
	if errResolve != nil {
		return ResinClearResult{AuthID: authID, Error: errResolve.Error()}, errResolve
	}
	stateAccount, _ := p.stateAccount(authID)
	generation := stateAccount.Resin.Generation
	proxyURL := strings.TrimSpace(authRawString(raw, "proxy_url"))
	cfg := p.ResinConfig()
	account, managed := managedResinBinding(raw, stateAccount, cfg)
	result := ResinClearResult{
		AuthID:   authID,
		Account:  account,
		ProxyURL: maskProxyURL(proxyURL),
	}
	hasManagedLease := managed || account != ""
	if proxyURL == "" && !hasManagedLease {
		result.Skipped = true
		return result, nil
	}
	delete(raw, "proxy_url")
	encoded, errMarshal := marshalRawMap(raw)
	if errMarshal != nil {
		result.Error = errMarshal.Error()
		return result, errMarshal
	}
	wantDelete := account != "" && !cfg.LocalOnly
	commit, errCommit := p.commitResinProxyChange(authID, response.Name, encoded, func(state *State) bool {
		stateAccount := p.ensureStateAccount(state, authID, "", "")
		stateAccount.Resin = ResinState{Managed: false, Generation: generation}
		if wantDelete {
			queueResinLeaseDeleteAndRecord(state, authID, cfg, account)
		}
		detail := "external proxy_url cleared"
		if account != "" {
			detail = "account=" + account
			if cfg.LocalOnly {
				detail += " remote_lease=skipped_local_only"
			} else {
				detail += " remote_lease=delete_queued"
			}
		}
		p.recordEvent(state, authID, "", "resin_clear", "cleared", detail)
		return wantDelete
	})
	if errCommit != nil {
		result.Error = sanitizeError(errCommit)
		return result, errCommit
	}
	result.Cleared = true
	result.RemoteLeaseDeleteQueued = commit.DeleteQueued
	return result, nil
}

type resinProxyCommit struct {
	DeleteQueued bool
}

// commitResinProxyChange saves the auth first because proxy_url is the routing
// source of truth. State persistence and lease cleanup are secondary to that
// local switch and must not turn a successful auth write into a failed rotation.
func (p *Plugin) commitResinProxyChange(authID, authName string, encoded json.RawMessage, update func(*State) bool) (resinProxyCommit, error) {
	commit := resinProxyCommit{}
	if _, errSave := p.saveAuth(authName, encoded); errSave != nil {
		return commit, errSave
	}
	store := p.stateStore()
	if store == nil {
		_ = p.recordInternalError(authID, fmt.Errorf("state store is not initialized"))
		return commit, nil
	}
	if errUpdate := store.UpdateIfChanged(func(state *State) error {
		commit.DeleteQueued = update(state)
		return nil
	}); errUpdate != nil {
		commit.DeleteQueued = false
		_ = p.recordInternalError(authID, errUpdate)
		return commit, nil
	}
	if commit.DeleteQueued {
		p.wakeResinLeaseDeleteLoop()
	}
	return commit, nil
}

func (p *Plugin) BindUnboundResin(ctx context.Context, dryRun bool) ([]ResinBindResult, error) {
	cfg := p.ResinConfig()
	if !cfg.Enabled {
		return nil, fmt.Errorf("resin is disabled")
	}
	if cfg.LocalOnly {
		return nil, fmt.Errorf("resin API mode is required to bind new proxies")
	}
	if errRefresh := p.RefreshCatalog(ctx); errRefresh != nil {
		return nil, errRefresh
	}
	accounts := p.catalogSnapshot()
	results := make([]ResinBindResult, len(accounts))
	attempted := make([]bool, len(accounts))
	indices := make([]int, len(accounts))
	for index := range accounts {
		indices[index] = index
	}
	errRun := runBoundedQueue(ctx, indices, p.probeQueueSize(), p.probeConcurrency(), func(jobCtx context.Context, index int) {
		account := accounts[index]
		_, _, raw, errResolve := p.resolveAuth(jobCtx, account.ID)
		if errResolve != nil {
			results[index] = ResinBindResult{AuthID: account.ID, DryRun: dryRun, Error: errResolve.Error()}
			attempted[index] = true
			return
		}
		stateAccount, _ := p.stateAccount(account.ID)
		if stateAccount.Resin.Account != "" || strings.TrimSpace(authRawString(raw, "proxy_url")) != "" {
			return
		}
		result, errBind := p.BindResin(jobCtx, account.ID, dryRun)
		if errBind != nil && result.Error == "" {
			result.Error = errBind.Error()
		}
		results[index] = result
		attempted[index] = true
	})
	if errRun != nil {
		return nil, errRun
	}
	filtered := make([]ResinBindResult, 0, len(results))
	for index, result := range results {
		if attempted[index] {
			filtered = append(filtered, result)
		}
	}
	return filtered, nil
}

func (p *Plugin) RotateResin(ctx context.Context, authID string) error {
	cfg := p.ResinConfig()
	if !cfg.Enabled {
		return fmt.Errorf("resin is disabled")
	}
	lock := p.accountLock(authID)
	lock.Lock()
	defer lock.Unlock()
	_, response, raw, errResolve := p.resolveAuth(ctx, authID)
	if errResolve != nil {
		return errResolve
	}
	stateAccount, _ := p.stateAccount(authID)
	previousAccount, managed := managedResinBinding(raw, stateAccount, cfg)
	if !managed {
		return fmt.Errorf("auth proxy is not managed by resin")
	}
	proxyURL := strings.TrimSpace(authRawString(raw, "proxy_url"))
	if proxyURL == "" && previousAccount == "" {
		return fmt.Errorf("auth has no proxy_url to rotate")
	}
	newAccount, errGenerate := p.generateResinAccountAvoiding(authID, raw, previousAccount)
	if errGenerate != nil {
		return errGenerate
	}
	deleteQueued := false
	if !cfg.LocalOnly && previousAccount != "" {
		renderedProxyURL, errRender := renderResinProxy(cfg, newAccount)
		if errRender != nil {
			return errRender
		}
		proxyURL = renderedProxyURL
		deleteQueued = true
	} else {
		if proxyURL == "" {
			return fmt.Errorf("local proxy rotation requires an existing proxy_url")
		}
		rotatedProxyURL, errRotateProxy := rewriteResinProxyAccount(proxyURL, newAccount)
		if errRotateProxy != nil {
			return errRotateProxy
		}
		proxyURL = rotatedProxyURL
	}
	generation := stateAccount.Resin.Generation + 1
	setRawString(raw, "proxy_url", proxyURL)
	encoded, errMarshal := marshalRawMap(raw)
	if errMarshal != nil {
		return errMarshal
	}
	oldLabel := previousAccount
	if oldLabel == "" {
		oldLabel = "unknown"
	}
	detail := fmt.Sprintf("旧代理账号：%s → 新代理账号：%s，未调用 Resin API", oldLabel, newAccount)
	if deleteQueued {
		detail = fmt.Sprintf("旧租约：%s → 新租约：%s，旧租约删除已入队", oldLabel, newAccount)
	}
	_, errCommit := p.commitResinProxyChange(authID, response.Name, encoded, func(state *State) bool {
		stateAccount := p.ensureStateAccount(state, authID, "", "")
		stateAccount.Resin = ResinState{Managed: true, Account: newAccount, Generation: generation}
		if deleteQueued {
			queueResinLeaseDeleteAndRecord(state, authID, cfg, previousAccount)
		}
		p.recordEvent(state, authID, "", "resin_rotate", "rotated", detail)
		return deleteQueued
	})
	if errCommit != nil {
		return errCommit
	}
	return nil
}

func resinAccountFromProxyURL(proxyURL string) string {
	parsed, errParse := url.Parse(strings.TrimSpace(proxyURL))
	if errParse != nil || parsed.User == nil {
		return ""
	}
	username := parsed.User.Username()
	dot := strings.LastIndexByte(username, '.')
	if dot <= 0 || dot >= len(username)-1 {
		return ""
	}
	return username[dot+1:]
}

func managedResinBinding(raw map[string]json.RawMessage, stateAccount AccountState, cfg ResinSettings) (string, bool) {
	storedAccount := strings.TrimSpace(stateAccount.Resin.Account)
	proxyURL := strings.TrimSpace(authRawString(raw, "proxy_url"))
	proxyAccount := resinAccountFromProxyURL(proxyURL)
	managed := stateAccount.Resin.Managed || storedAccount != ""
	if managed {
		if proxyAccount != "" {
			return proxyAccount, true
		}
		return storedAccount, true
	}
	if resinProxyMatchesSettings(proxyURL, cfg) {
		return proxyAccount, proxyAccount != ""
	}
	return "", false
}

func resinProxyMatchesSettings(proxyURL string, cfg ResinSettings) bool {
	if strings.TrimSpace(proxyURL) == "" || cfg.LocalOnly {
		return false
	}
	parsedProxy, errProxy := url.Parse(strings.TrimSpace(proxyURL))
	if errProxy != nil || parsedProxy.Scheme == "" || parsedProxy.Host == "" {
		return false
	}
	rendered, errRender := renderResinProxy(cfg, "grok-validation")
	if errRender != nil {
		return false
	}
	parsedSettings, errSettings := url.Parse(rendered)
	if errSettings != nil || parsedSettings.Scheme == "" || parsedSettings.Host == "" {
		return false
	}
	return parsedProxy.Scheme == parsedSettings.Scheme && strings.EqualFold(parsedProxy.Host, parsedSettings.Host)
}

func rewriteResinProxyAccount(proxyURL, account string) (string, error) {
	account = strings.TrimSpace(account)
	if account == "" {
		return "", fmt.Errorf("resin account is empty")
	}
	parsed, errParse := url.Parse(strings.TrimSpace(proxyURL))
	if errParse != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User == nil {
		return "", fmt.Errorf("proxy_url is invalid")
	}
	username := parsed.User.Username()
	password, hasPassword := parsed.User.Password()
	if username == "" || !hasPassword {
		return "", fmt.Errorf("proxy_url must contain username and password")
	}
	dot := strings.LastIndexByte(username, '.')
	if dot <= 0 || dot >= len(username)-1 {
		return "", fmt.Errorf("proxy_url username does not contain a resin account")
	}
	parsed.User = url.UserPassword(username[:dot+1]+account, password)
	return parsed.String(), nil
}

func (p *Plugin) generateResinAccount(authID string, raw map[string]json.RawMessage) (string, error) {
	stateAccount, _ := p.stateAccount(authID)
	return p.generateResinAccountAvoiding(authID, raw, stateAccount.Resin.Account)
}

func (p *Plugin) generateResinAccountAvoiding(authID string, raw map[string]json.RawMessage, oldAccount string) (string, error) {
	base := resinAccountBase(raw)
	state := p.stateStore().Snapshot()
	used := make(map[string]struct{}, len(state.Accounts))
	for _, account := range state.Accounts {
		if account != nil && account.Resin.Account != "" {
			used[account.Resin.Account] = struct{}{}
		}
	}
	for attempt := 0; attempt < 32; attempt++ {
		suffix, errSuffix := randomBase36(4)
		if errSuffix != nil {
			return "", errSuffix
		}
		candidate := "grok-" + base + "-" + suffix
		if len(candidate) > 128 {
			candidate = candidate[:123] + "-" + suffix
		}
		if candidate == oldAccount {
			continue
		}
		if _, exists := used[candidate]; exists {
			continue
		}
		return candidate, nil
	}
	stateAccount, _ := p.stateAccount(authID)
	suffixSeed := fmt.Sprintf("%s|%d|%d", authID, time.Now().UnixNano(), stateAccount.Resin.Generation)
	sum := sha256.Sum256([]byte(suffixSeed))
	for offset := 0; offset+4 <= len(sum); offset += 4 {
		candidate := "grok-" + base + "-" + base36FromBytes(sum[offset:offset+4], 4)
		if candidate == oldAccount {
			continue
		}
		if _, exists := used[candidate]; exists {
			continue
		}
		return candidate, nil
	}
	return "", fmt.Errorf("generate unique resin account failed")
}

func resinAccountBase(raw map[string]json.RawMessage) string {
	source := authEmail(raw)
	if source == "" {
		source = authLabel(raw)
	}
	if source == "" {
		source = "auth_" + authSubjectHash(raw)
	}
	if at := strings.IndexByte(source, '@'); at >= 0 {
		source = source[:at]
	}
	source = strings.ToLower(strings.TrimSpace(source))
	source = resinAccountChars.ReplaceAllString(source, "_")
	for strings.Contains(source, "__") {
		source = strings.ReplaceAll(source, "__", "_")
	}
	source = strings.Trim(source, "_")
	if source == "" {
		source = "auth_" + authSubjectHash(raw)
	}
	if len(source) > 48 {
		source = source[:48]
		source = strings.TrimRight(source, "_")
	}
	return source
}

func randomBase36(length int) (string, error) {
	if length <= 0 {
		return "", nil
	}
	limit := big.NewInt(int64(len(base36Alphabet)))
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		value, errValue := rand.Int(rand.Reader, limit)
		if errValue != nil {
			return "", errValue
		}
		result[i] = base36Alphabet[value.Int64()]
	}
	return string(result), nil
}

func base36FromBytes(bytes []byte, length int) string {
	if length <= 0 || len(bytes) == 0 {
		return ""
	}
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = base36Alphabet[int(bytes[i%len(bytes)])%len(base36Alphabet)]
	}
	return string(result)
}

func renderResinProxy(cfg ResinSettings, account string) (string, error) {
	token := strings.TrimSpace(cfg.ProxyToken)
	if token == "" {
		return "", fmt.Errorf("resin proxy_token is not configured")
	}
	template := cfg.ProxyTemplate
	if strings.TrimSpace(template) == "" {
		return "", fmt.Errorf("resin proxy_template is empty")
	}
	if !strings.Contains(template, "{account}") {
		return "", fmt.Errorf("resin proxy_template must contain {account}")
	}
	result := strings.ReplaceAll(template, "{platform}", cfg.Platform)
	result = strings.ReplaceAll(result, "{account}", account)
	result = strings.ReplaceAll(result, "{proxy_token}", "proxy-token")
	parsed, errParse := url.Parse(result)
	if errParse != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User == nil {
		return "", fmt.Errorf("render resin proxy template failed")
	}
	parsed.User = url.UserPassword(parsed.User.Username(), token)
	return parsed.String(), nil
}

func deleteResinLease(ctx context.Context, adminURL, platformID, adminToken, account string) error {
	base := strings.TrimRight(strings.TrimSpace(adminURL), "/")
	endpoint := base + "/api/v1/platforms/" + url.PathEscape(platformID) + "/leases/" + url.PathEscape(account)
	req, errNew := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if errNew != nil {
		return errNew
	}
	req.Header.Set("Authorization", "Bearer "+adminToken)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, errDo := client.Do(req)
	if errDo != nil {
		return errDo
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		return &resinDeleteError{Status: resp.StatusCode, Body: strings.TrimSpace(string(body))}
	}
}

func maskProxyURL(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	parsed, errParse := url.Parse(value)
	if errParse != nil {
		return "[redacted]"
	}
	if parsed.Host == "" {
		return parsed.Scheme + "://[redacted]"
	}
	return parsed.Scheme + "://" + parsed.Host
}

func maskAccountIdentity(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.Contains(value, "@") {
		return maskEmail(value)
	}
	return maskAuthID(value)
}

func maskEmail(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	at := strings.IndexByte(value, '@')
	if at <= 0 {
		return maskAuthID(value)
	}
	local := value[:at]
	domain := value[at:]
	switch {
	case len(local) <= 2:
		return local[:1] + "***" + domain
	case len(local) >= 8:
		return local[:3] + "***" + local[len(local)-2:] + domain
	default:
		return local[:2] + "***" + local[len(local)-1:] + domain
	}
}

func maskAuthID(value string) string {
	if len(value) <= 12 {
		return value
	}
	return value[:12] + "..." + value[len(value)-4:]
}
