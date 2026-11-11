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
	_, response, raw, errResolve := p.resolveAuth(ctx, authID)
	if errResolve != nil {
		return ResinBindResult{AuthID: authID, DryRun: dryRun, Error: errResolve.Error()}, errResolve
	}
	meta := guardMeta(raw)
	existingProxy := strings.TrimSpace(authRawString(raw, "proxy_url"))
	if existingProxy != "" && !meta.Managed {
		return ResinBindResult{AuthID: authID, DryRun: dryRun, Conflict: true, Error: "auth already has a non-managed proxy_url"}, fmt.Errorf("proxy_url conflict")
	}
	if meta.Managed && meta.ResinAccount != "" {
		return ResinBindResult{AuthID: authID, Account: meta.ResinAccount, ProxyURL: maskProxyURL(existingProxy), DryRun: dryRun}, nil
	}
	generated, errGenerate := p.generateResinAccount(ctx, authID, raw)
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
	meta.Managed = true
	meta.ResinAccount = generated
	meta.ResinGeneration++
	setRawString(raw, "proxy_url", proxyURL)
	if errMeta := setGuardMeta(raw, meta); errMeta != nil {
		return result, errMeta
	}
	encoded, errMarshal := marshalRawMap(raw)
	if errMarshal != nil {
		return result, errMarshal
	}
	if _, errSave := p.saveAuth(response.Name, encoded); errSave != nil {
		return result, errSave
	}
	if errUpdate := p.updateStateAccount(authID, func(stateAccount *AccountState) error {
		stateAccount.Resin = ResinState{Managed: true, Account: generated, Generation: meta.ResinGeneration}
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

func (p *Plugin) BindUnboundResin(ctx context.Context, dryRun bool) ([]ResinBindResult, error) {
	if errRefresh := p.RefreshCatalog(ctx); errRefresh != nil {
		return nil, errRefresh
	}
	results := make([]ResinBindResult, 0)
	for _, account := range p.catalogSnapshot() {
		_, _, raw, errResolve := p.resolveAuth(ctx, account.ID)
		if errResolve != nil {
			results = append(results, ResinBindResult{AuthID: account.ID, DryRun: dryRun, Error: errResolve.Error()})
			continue
		}
		meta := guardMeta(raw)
		if meta.ResinAccount != "" || strings.TrimSpace(authRawString(raw, "proxy_url")) != "" {
			continue
		}
		result, errBind := p.BindResin(ctx, account.ID, dryRun)
		if errBind != nil && result.Error == "" {
			result.Error = errBind.Error()
		}
		results = append(results, result)
	}
	return results, nil
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
	meta := guardMeta(raw)
	if !meta.Managed || strings.TrimSpace(meta.ResinAccount) == "" {
		return fmt.Errorf("auth is not managed by resin")
	}
	previousAccount := meta.ResinAccount
	operationCfg, errResolvePlatform := resolveResinPlatformForOperation(ctx, cfg)
	if errResolvePlatform != nil {
		return errResolvePlatform
	}
	adminToken := strings.TrimSpace(operationCfg.AdminToken)
	if errDelete := deleteResinLease(ctx, operationCfg.AdminURL, operationCfg.PlatformID, adminToken, meta.ResinAccount); errDelete != nil {
		if typed, ok := errDelete.(*resinDeleteError); ok && (typed.Status == 401 || typed.Status == 403) {
			return fmt.Errorf("resin admin authentication failed: status %d", typed.Status)
		}
		return errDelete
	}
	newAccount, errGenerate := p.generateResinAccount(ctx, authID, raw)
	if errGenerate != nil {
		return errGenerate
	}
	proxyURL, errRender := renderResinProxy(operationCfg, newAccount)
	if errRender != nil {
		return errRender
	}
	meta.Managed = true
	meta.ResinAccount = newAccount
	meta.ResinGeneration++
	setRawString(raw, "proxy_url", proxyURL)
	if errMeta := setGuardMeta(raw, meta); errMeta != nil {
		return errMeta
	}
	encoded, errMarshal := marshalRawMap(raw)
	if errMarshal != nil {
		return errMarshal
	}
	if _, errSave := p.saveAuth(response.Name, encoded); errSave != nil {
		return errSave
	}
	if errUpdate := p.updateStateAccount(authID, func(stateAccount *AccountState) error {
		stateAccount.Resin = ResinState{Managed: true, Account: newAccount, Generation: meta.ResinGeneration}
		return nil
	}); errUpdate != nil {
		return errUpdate
	}
	_ = p.stateStore().Update(func(state *State) error {
		p.recordEvent(state, authID, "", "resin_rotate", "rotated", fmt.Sprintf("旧租约：%s → 新租约：%s", previousAccount, newAccount))
		return nil
	})
	return nil
}

func (p *Plugin) generateResinAccount(ctx context.Context, authID string, raw map[string]json.RawMessage) (string, error) {
	base := resinAccountBase(raw)
	oldAccount := guardMeta(raw).ResinAccount
	state := p.stateStore().Snapshot()
	used := make(map[string]struct{}, len(state.Accounts))
	for _, account := range state.Accounts {
		if account != nil && account.Resin.Account != "" {
			used[account.Resin.Account] = struct{}{}
		}
	}
	for _, catalogAccount := range p.catalogSnapshot() {
		if catalogAccount.ID == authID {
			continue
		}
		_, _, otherRaw, err := p.resolveAuth(ctx, catalogAccount.ID)
		if err == nil {
			if value := guardMeta(otherRaw).ResinAccount; value != "" {
				used[value] = struct{}{}
			}
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
	suffixSeed := fmt.Sprintf("%s|%d|%d", authID, time.Now().UnixNano(), guardMeta(raw).ResinGeneration)
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

func maskEmail(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	at := strings.IndexByte(value, '@')
	if at <= 0 {
		if len(value) <= 1 {
			return "*"
		}
		return value[:1] + "***"
	}
	return value[:1] + "***" + value[at:]
}

func maskAuthID(value string) string {
	if len(value) <= 8 {
		return value
	}
	return value[:8] + "..."
}
