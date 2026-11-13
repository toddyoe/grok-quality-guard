package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

var resinPlatformIDPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type ResinSettings struct {
	Configured                    bool      `json:"configured"`
	Enabled                       bool      `json:"enabled"`
	LocalOnly                     bool      `json:"local_only"`
	Platform                      string    `json:"platform"`
	PlatformID                    string    `json:"platform_id"`
	AdminURL                      string    `json:"admin_url"`
	ProxyTemplate                 string    `json:"proxy_template"`
	AutoRotateAfterQualityFailure bool      `json:"auto_rotate_after_quality_failure"`
	AdminToken                    string    `json:"admin_token,omitempty"`
	ProxyToken                    string    `json:"proxy_token,omitempty"`
	ValidatedAt                   time.Time `json:"validated_at,omitempty"`
}

type resinConfigView struct {
	Configured                    bool      `json:"configured"`
	Enabled                       bool      `json:"enabled"`
	LocalOnly                     bool      `json:"local_only"`
	Platform                      string    `json:"platform"`
	PlatformID                    string    `json:"platform_id"`
	AdminURL                      string    `json:"admin_url"`
	ProxyTemplate                 string    `json:"proxy_template"`
	AutoRotateAfterQualityFailure bool      `json:"auto_rotate_after_quality_failure"`
	AdminTokenSet                 bool      `json:"admin_token_set"`
	ProxyTokenSet                 bool      `json:"proxy_token_set"`
	ValidatedAt                   time.Time `json:"validated_at,omitempty"`
}

type resinValidationResult struct {
	LocalOnly         bool      `json:"local_only"`
	AdminAPIReachable bool      `json:"admin_api_reachable"`
	AdminTokenValid   bool      `json:"admin_token_valid"`
	PlatformValid     bool      `json:"platform_valid"`
	Platform          string    `json:"platform"`
	PlatformID        string    `json:"platform_id,omitempty"`
	ProxyTokenValid   bool      `json:"proxy_token_valid"`
	ValidatedAt       time.Time `json:"validated_at"`
}

type resinConfigError struct {
	Status  int
	Message string
}

func (e *resinConfigError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func resinConfigErrorf(status int, format string, args ...any) error {
	return &resinConfigError{Status: status, Message: fmt.Sprintf(format, args...)}
}

func (p *Plugin) ResinConfig() ResinSettings {
	p.mu.RLock()
	cfg := p.cfg.Resin
	p.mu.RUnlock()

	settings := resinSettingsFromConfig(cfg)
	store := p.stateStore()
	if store != nil {
		state := store.Snapshot()
		if state.Resin.Configured {
			settings = state.Resin
			settings.Configured = true
			if !settings.LocalOnly {
				if strings.TrimSpace(settings.AdminToken) == "" {
					settings.AdminToken = strings.TrimSpace(os.Getenv(cfg.AdminTokenEnv))
				}
				if strings.TrimSpace(settings.ProxyToken) == "" {
					settings.ProxyToken = strings.TrimSpace(os.Getenv(cfg.ProxyTokenEnv))
				}
			}

		}
	}
	return normalizeResinSettingsBestEffort(settings)
}

func resinSettingsFromConfig(cfg ResinConfig) ResinSettings {
	return normalizeResinSettingsBestEffort(ResinSettings{
		Enabled:                       cfg.Enabled,
		LocalOnly:                     cfg.LocalOnly,
		Platform:                      cfg.Platform,
		PlatformID:                    cfg.PlatformID,
		AdminURL:                      cfg.AdminURL,
		ProxyTemplate:                 cfg.ProxyTemplate,
		AutoRotateAfterQualityFailure: cfg.AutoRotateAfterQualityFailure,
		AdminToken:                    strings.TrimSpace(os.Getenv(cfg.AdminTokenEnv)),
		ProxyToken:                    strings.TrimSpace(os.Getenv(cfg.ProxyTokenEnv)),
	})
}

func normalizeResinSettingsBestEffort(settings ResinSettings) ResinSettings {
	settings.Platform = strings.TrimSpace(settings.Platform)
	if settings.Platform == "" {
		settings.Platform = "Default"
	}
	settings.PlatformID = strings.TrimSpace(settings.PlatformID)
	settings.AdminURL = strings.TrimRight(strings.TrimSpace(settings.AdminURL), "/")
	settings.ProxyTemplate = strings.TrimSpace(settings.ProxyTemplate)
	if settings.ProxyTemplate == "" {
		settings.ProxyTemplate = "socks5h://{platform}.{account}:{proxy_token}@resin:2260"
	}
	settings.AdminToken = strings.TrimSpace(settings.AdminToken)
	settings.ProxyToken = strings.TrimSpace(settings.ProxyToken)
	if settings.LocalOnly {
		settings.PlatformID = ""
		settings.AdminURL = ""
		settings.AdminToken = ""
		settings.ProxyToken = ""
	}

	return settings
}

func (p *Plugin) resinConfigView(settings ResinSettings) resinConfigView {
	return resinConfigView{
		Configured:                    settings.Configured,
		Enabled:                       settings.Enabled,
		LocalOnly:                     settings.LocalOnly,
		Platform:                      settings.Platform,
		PlatformID:                    settings.PlatformID,
		AdminURL:                      settings.AdminURL,
		ProxyTemplate:                 settings.ProxyTemplate,
		AutoRotateAfterQualityFailure: settings.AutoRotateAfterQualityFailure,
		AdminTokenSet:                 strings.TrimSpace(settings.AdminToken) != "",
		ProxyTokenSet:                 strings.TrimSpace(settings.ProxyToken) != "",
		ValidatedAt:                   settings.ValidatedAt,
	}
}

func (p *Plugin) actionResinConfigGet() (any, int, error) {
	return p.resinConfigView(p.ResinConfig()), http.StatusOK, nil
}

func (p *Plugin) actionResinConfigValidate(_ context.Context, params map[string]any) (any, int, error) {
	candidate := resinSettingsFromParams(params, p.ResinConfig())
	normalized, errNormalize := normalizeResinSettings(candidate)
	if errNormalize != nil {
		return nil, http.StatusBadRequest, errNormalize
	}
	task := p.startTask("resin.config.validate", nil, "正在验证 Resin 配置", func(taskCtx context.Context) (any, error) {
		return validateResinSettings(taskCtx, normalized)
	})
	return task, http.StatusAccepted, nil
}

func (p *Plugin) actionResinConfigSave(_ context.Context, params map[string]any) (any, int, error) {
	if p.stateStore() == nil {
		return nil, http.StatusInternalServerError, errors.New("state store is not initialized")
	}
	candidate := resinSettingsFromParams(params, p.ResinConfig())
	normalized, errNormalize := normalizeResinSettings(candidate)
	if errNormalize != nil {
		return nil, http.StatusBadRequest, errNormalize
	}
	task := p.startTask("resin.config.save", nil, "正在验证并保存 Resin 配置", func(taskCtx context.Context) (any, error) {
		return p.saveResinConfig(taskCtx, normalized)
	})
	return task, http.StatusAccepted, nil
}

func (p *Plugin) saveResinConfig(ctx context.Context, normalized ResinSettings) (any, error) {
	store := p.stateStore()
	if store == nil {
		return nil, errors.New("state store is not initialized")
	}
	result, errValidate := validateResinSettings(ctx, normalized)
	if errValidate != nil {
		return nil, errValidate
	}
	if result.Platform != "" {
		normalized.Platform = result.Platform
	}
	if result.PlatformID != "" {
		normalized.PlatformID = result.PlatformID
	}
	normalized.LocalOnly = result.LocalOnly
	normalized.Configured = true
	normalized.ValidatedAt = result.ValidatedAt
	eventResult := "validated"
	eventDetail := "platform_id=" + normalized.PlatformID
	if normalized.LocalOnly {
		eventResult = "local_mode"
		eventDetail = "local_only"
	}
	if errStore := store.Update(func(state *State) error {
		state.Resin = normalized
		state.Policy.ResinAutoRotateAfterQualityFail = normalized.AutoRotateAfterQualityFailure
		p.recordEvent(state, "", "", "resin_config_save", eventResult, eventDetail)
		return nil
	}); errStore != nil {
		return nil, errStore
	}
	p.mu.Lock()
	p.policy.ResinAutoRotateAfterQualityFail = normalized.AutoRotateAfterQualityFailure
	p.cfg.Resin.Enabled = normalized.Enabled
	p.cfg.Resin.LocalOnly = normalized.LocalOnly
	p.cfg.Resin.Platform = normalized.Platform
	p.cfg.Resin.PlatformID = normalized.PlatformID
	p.cfg.Resin.AdminURL = normalized.AdminURL
	p.cfg.Resin.ProxyTemplate = normalized.ProxyTemplate
	p.cfg.Resin.AutoRotateAfterQualityFailure = normalized.AutoRotateAfterQualityFailure
	p.mu.Unlock()
	return p.resinConfigView(normalized), nil
}
func resinSettingsFromParams(params map[string]any, current ResinSettings) ResinSettings {
	settings := current
	settings.Enabled = optionalBoolParam(params, "enabled", settings.Enabled)
	settings.LocalOnly = optionalBoolParam(params, "local_only", settings.LocalOnly)
	settings.Platform = optionalStringParam(params, "platform", settings.Platform)
	settings.PlatformID = optionalStringParam(params, "platform_id", settings.PlatformID)
	settings.AdminURL = optionalStringParam(params, "admin_url", settings.AdminURL)
	settings.ProxyTemplate = optionalStringParam(params, "proxy_template", settings.ProxyTemplate)
	settings.AutoRotateAfterQualityFailure = optionalBoolParam(params, "auto_rotate_after_quality_failure", settings.AutoRotateAfterQualityFailure)
	if value, ok := params["admin_token"]; ok {
		if token := strings.TrimSpace(asString(value)); token != "" {
			settings.AdminToken = token
		}
	}
	if value, ok := params["proxy_token"]; ok {
		if token := strings.TrimSpace(asString(value)); token != "" {
			settings.ProxyToken = token
		}
	}
	if settings.LocalOnly {
		settings.PlatformID = ""
		settings.AdminURL = ""
		settings.AdminToken = ""
		settings.ProxyToken = ""
	}
	return settings
}

func optionalStringParam(params map[string]any, key, fallback string) string {
	value, ok := params[key]
	if !ok || value == nil {
		return fallback
	}
	return asString(value)
}

func optionalBoolParam(params map[string]any, key string, fallback bool) bool {
	value, ok := params[key]
	if !ok || value == nil {
		return fallback
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return fallback
}

func normalizeResinAdminSettings(settings ResinSettings) (ResinSettings, error) {
	settings = normalizeResinSettingsBestEffort(settings)
	if settings.Platform == "" {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin platform is required")
	}
	if settings.AdminURL == "" {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin admin_url is required")
	}
	parsedAdmin, errAdmin := url.Parse(settings.AdminURL)
	if errAdmin != nil || parsedAdmin.Scheme == "" || parsedAdmin.Host == "" {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin admin_url is invalid")
	}
	if parsedAdmin.Scheme != "http" && parsedAdmin.Scheme != "https" {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin admin_url must use http or https")
	}
	if parsedAdmin.User != nil || parsedAdmin.RawQuery != "" || parsedAdmin.Fragment != "" {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin admin_url must not contain userinfo, query, or fragment")
	}
	settings.AdminURL = strings.TrimRight(parsedAdmin.String(), "/")
	if settings.AdminToken == "" {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin admin_token is required")
	}
	if len(settings.AdminToken) > 4096 {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin admin_token is too long")
	}
	return settings, nil
}

func normalizeResinSettings(settings ResinSettings) (ResinSettings, error) {
	settings = normalizeResinSettingsBestEffort(settings)
	if settings.LocalOnly {
		return settings, nil
	}
	normalized, errAdmin := normalizeResinAdminSettings(settings)
	if errAdmin != nil {
		return ResinSettings{}, errAdmin
	}
	settings = normalized
	if settings.ProxyToken == "" {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin proxy_token is required")
	}
	if len(settings.ProxyToken) > 255 {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin proxy_token must not exceed 255 bytes")
	}
	if !strings.Contains(settings.ProxyTemplate, "{platform}") || !strings.Contains(settings.ProxyTemplate, "{account}") || !strings.Contains(settings.ProxyTemplate, "{proxy_token}") {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin proxy_template must contain {platform}, {account}, and {proxy_token}")
	}
	proxyURL, errRender := renderResinProxy(settings, "grok-validation")
	if errRender != nil {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin proxy_template is invalid")
	}
	parsedProxy, errProxy := url.Parse(proxyURL)
	if errProxy != nil || parsedProxy.Host == "" || parsedProxy.User == nil {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin proxy_template must contain a proxy host and userinfo")
	}
	if parsedProxy.Scheme != "socks5" && parsedProxy.Scheme != "socks5h" {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin proxy_template must use socks5 or socks5h")
	}
	username := parsedProxy.User.Username()
	_, hasPassword := parsedProxy.User.Password()
	if username == "" || !hasPassword {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin proxy_template must contain username and password")
	}
	if len(username) > 255 {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin proxy username must not exceed 255 bytes")
	}
	if parsedProxy.Port() == "" {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin proxy_template must include a port")
	}
	return settings, nil
}

func validateResinSettings(ctx context.Context, settings ResinSettings) (resinValidationResult, error) {
	result := resinValidationResult{}
	if settings.LocalOnly {
		result.LocalOnly = true
		result.ValidatedAt = time.Now().UTC()
		return result, nil
	}
	if errAdmin := validateResinAdminAPI(ctx, settings); errAdmin != nil {
		return result, errAdmin
	}
	result.AdminAPIReachable = true
	result.AdminTokenValid = true
	resolved, errPlatform := resolveResinPlatformByName(ctx, settings)
	if errPlatform != nil {
		return result, errPlatform
	}
	settings = resolved
	result.Platform = settings.Platform
	result.PlatformID = settings.PlatformID
	result.PlatformValid = true
	if errProxy := validateResinProxyToken(ctx, settings); errProxy != nil {
		return result, errProxy
	}
	result.ProxyTokenValid = true
	result.ValidatedAt = time.Now().UTC()
	return result, nil
}

func validateResinAdminAPI(ctx context.Context, settings ResinSettings) error {
	status, _, errRequest := resinAdminRequest(ctx, settings, http.MethodGet, "/api/v1/system/info")
	if errRequest != nil {
		return resinConfigErrorf(http.StatusBadGateway, "resin admin API is unreachable: %s", sanitizeError(errRequest))
	}
	switch status {
	case http.StatusOK:
		return nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return resinConfigErrorf(http.StatusBadRequest, "resin admin token was rejected with status %d", status)
	default:
		if status >= 500 {
			return resinConfigErrorf(http.StatusBadGateway, "resin admin API returned status %d", status)
		}
		return resinConfigErrorf(http.StatusBadRequest, "resin admin API returned status %d", status)
	}
}

func validateResinPlatform(ctx context.Context, settings ResinSettings) (string, error) {
	path := "/api/v1/platforms/" + url.PathEscape(settings.PlatformID)
	status, body, errRequest := resinAdminRequest(ctx, settings, http.MethodGet, path)
	if errRequest != nil {
		return "", resinConfigErrorf(http.StatusBadGateway, "resin platform validation failed: %s", sanitizeError(errRequest))
	}
	switch status {
	case http.StatusOK:
		var platform struct {
			Name string `json:"name"`
		}
		if errUnmarshal := json.Unmarshal(body, &platform); errUnmarshal != nil || strings.TrimSpace(platform.Name) == "" {
			return "", resinConfigErrorf(http.StatusBadGateway, "resin platform API returned an invalid response")
		}
		return platform.Name, nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return "", resinConfigErrorf(http.StatusBadRequest, "resin admin token was rejected with status %d", status)
	case http.StatusNotFound:
		return "", resinConfigErrorf(http.StatusBadRequest, "resin platform_id was not found")
	default:
		if status >= 500 {
			return "", resinConfigErrorf(http.StatusBadGateway, "resin platform API returned status %d", status)
		}
		return "", resinConfigErrorf(http.StatusBadRequest, "resin platform API returned status %d", status)
	}
}

type resinPlatformSummary struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func resolveResinPlatformByName(ctx context.Context, settings ResinSettings) (ResinSettings, error) {
	requested := strings.TrimSpace(settings.Platform)
	if requested == "" {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin platform is required")
	}
	query := url.Values{}
	query.Set("keyword", requested)
	query.Set("sort_by", "name")
	query.Set("sort_order", "asc")
	query.Set("limit", "1000")
	query.Set("offset", "0")
	status, body, errRequest := resinAdminRequest(ctx, settings, http.MethodGet, "/api/v1/platforms?"+query.Encode())
	if errRequest != nil {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadGateway, "resin platform validation failed: %s", sanitizeError(errRequest))
	}
	switch status {
	case http.StatusOK:
		var page struct {
			Items []resinPlatformSummary `json:"items"`
		}
		if errUnmarshal := json.Unmarshal(body, &page); errUnmarshal != nil {
			return ResinSettings{}, resinConfigErrorf(http.StatusBadGateway, "resin platform API returned an invalid response")
		}
		exact := make([]resinPlatformSummary, 0, 1)
		folded := make([]resinPlatformSummary, 0, 1)
		for _, platform := range page.Items {
			if platform.Name == requested {
				exact = append(exact, platform)
			}
			if strings.EqualFold(platform.Name, requested) {
				folded = append(folded, platform)
			}
		}
		matches := exact
		if len(matches) == 0 {
			matches = folded
		}
		if len(matches) == 0 {
			return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin platform name was not found")
		}
		if len(matches) > 1 {
			return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin platform name is ambiguous")
		}
		matched := matches[0]
		if !resinPlatformIDPattern.MatchString(matched.ID) || strings.TrimSpace(matched.Name) == "" {
			return ResinSettings{}, resinConfigErrorf(http.StatusBadGateway, "resin platform API returned an invalid response")
		}
		settings.Platform = matched.Name
		settings.PlatformID = matched.ID
		return settings, nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin admin token was rejected with status %d", status)
	default:
		if status >= 500 {
			return ResinSettings{}, resinConfigErrorf(http.StatusBadGateway, "resin platform API returned status %d", status)
		}
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin platform API returned status %d", status)
	}
}

func resolveResinPlatformForOperation(ctx context.Context, settings ResinSettings) (ResinSettings, error) {
	normalized, errNormalize := normalizeResinSettings(settings)
	if errNormalize != nil {
		return ResinSettings{}, errNormalize
	}
	if normalized.LocalOnly {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin API mode is required for this operation")
	}
	return resolveResinPlatformReference(ctx, normalized)
}

func resolveResinPlatformReference(ctx context.Context, settings ResinSettings) (ResinSettings, error) {
	if strings.TrimSpace(settings.PlatformID) != "" {
		platformName, errPlatform := validateResinPlatform(ctx, settings)
		if errPlatform != nil {
			return ResinSettings{}, errPlatform
		}
		settings.Platform = platformName
		return settings, nil
	}
	return resolveResinPlatformByName(ctx, settings)
}

func resinAdminRequest(ctx context.Context, settings ResinSettings, method, path string) (int, []byte, error) {
	base := strings.TrimRight(settings.AdminURL, "/")
	req, errNew := http.NewRequestWithContext(ctx, method, base+path, nil)
	if errNew != nil {
		return 0, nil, errNew
	}
	req.Header.Set("Authorization", "Bearer "+settings.AdminToken)
	req.Header.Set("Accept", "application/json")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	client := &http.Client{Timeout: 10 * time.Second, Transport: transport}
	resp, errDo := client.Do(req)
	if errDo != nil {
		return 0, nil, errDo
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	return resp.StatusCode, body, nil
}

func validateResinProxyToken(ctx context.Context, settings ResinSettings) error {
	proxyURL, errRender := renderResinProxy(settings, "grok-validation")
	if errRender != nil {
		return resinConfigErrorf(http.StatusBadRequest, "resin proxy_template is invalid")
	}
	parsed, errParse := url.Parse(proxyURL)
	if errParse != nil || parsed.Host == "" || parsed.User == nil {
		return resinConfigErrorf(http.StatusBadRequest, "resin proxy_template is invalid")
	}
	username := parsed.User.Username()
	password, okPassword := parsed.User.Password()
	if !okPassword || username == "" || password == "" {
		return resinConfigErrorf(http.StatusBadRequest, "resin proxy_token is required")
	}
	hostPort := parsed.Host
	if parsed.Port() == "" {
		hostPort = net.JoinHostPort(parsed.Hostname(), "1080")
	}
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, errDial := dialer.DialContext(ctx, "tcp", hostPort)
	if errDial != nil {
		return resinConfigErrorf(http.StatusBadGateway, "resin proxy is unreachable: %s", sanitizeError(errDial))
	}
	defer conn.Close()
	deadline := time.Now().Add(10 * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if errDeadline := conn.SetDeadline(deadline); errDeadline != nil {
		return resinConfigErrorf(http.StatusBadGateway, "set resin proxy deadline: %s", sanitizeError(errDeadline))
	}
	if errHandshake := socks5Authenticate(conn, username, password); errHandshake != nil {
		if errors.Is(errHandshake, errSocks5AuthRejected) {
			return resinConfigErrorf(http.StatusBadRequest, "resin proxy token was rejected")
		}
		return resinConfigErrorf(http.StatusBadGateway, "resin proxy authentication failed: %s", sanitizeError(errHandshake))
	}
	return nil
}

var errSocks5AuthRejected = errors.New("socks5 authentication rejected")

func socks5Authenticate(conn net.Conn, username, password string) error {
	if conn == nil {
		return errors.New("socks5 connection is nil")
	}
	if len(username) == 0 || len(username) > 255 || len(password) == 0 || len(password) > 255 {
		return errors.New("socks5 credentials have invalid length")
	}
	if errWrite := writeAll(conn, []byte{0x05, 0x01, 0x02}); errWrite != nil {
		return errWrite
	}
	methodReply := make([]byte, 2)
	if _, errRead := io.ReadFull(conn, methodReply); errRead != nil {
		return errRead
	}
	if methodReply[0] != 0x05 {
		return errors.New("socks5 proxy returned an invalid version")
	}
	if methodReply[1] != 0x02 {
		return errors.New("socks5 proxy did not accept username/password authentication")
	}
	authRequest := make([]byte, 0, 3+len(username)+len(password))
	authRequest = append(authRequest, 0x01, byte(len(username)))
	authRequest = append(authRequest, username...)
	authRequest = append(authRequest, byte(len(password)))
	authRequest = append(authRequest, password...)
	if errWrite := writeAll(conn, authRequest); errWrite != nil {
		return errWrite
	}
	authReply := make([]byte, 2)
	if _, errRead := io.ReadFull(conn, authReply); errRead != nil {
		return errRead
	}
	if authReply[0] != 0x01 {
		return errors.New("socks5 proxy returned an invalid auth version")
	}
	if authReply[1] != 0x00 {
		return errSocks5AuthRejected
	}
	return nil
}

func writeAll(conn net.Conn, payload []byte) error {
	for len(payload) > 0 {
		written, errWrite := conn.Write(payload)
		if errWrite != nil {
			return errWrite
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}
