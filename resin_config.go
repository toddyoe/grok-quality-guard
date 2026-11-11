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
	AdminAPIReachable bool      `json:"admin_api_reachable"`
	AdminTokenValid   bool      `json:"admin_token_valid"`
	PlatformValid     bool      `json:"platform_valid"`
	Platform          string    `json:"platform"`
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
			if strings.TrimSpace(settings.AdminToken) == "" {
				settings.AdminToken = strings.TrimSpace(os.Getenv(cfg.AdminTokenEnv))
			}
			if strings.TrimSpace(settings.ProxyToken) == "" {
				settings.ProxyToken = strings.TrimSpace(os.Getenv(cfg.ProxyTokenEnv))
			}
		}
	}
	return normalizeResinSettingsBestEffort(settings)
}

func resinSettingsFromConfig(cfg ResinConfig) ResinSettings {
	return normalizeResinSettingsBestEffort(ResinSettings{
		Enabled:                       cfg.Enabled,
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
	return settings
}

func (p *Plugin) resinConfigView(settings ResinSettings) resinConfigView {
	return resinConfigView{
		Configured:                    settings.Configured,
		Enabled:                       settings.Enabled,
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

func (p *Plugin) actionResinConfigValidate(ctx context.Context, params map[string]any) (any, int, error) {
	candidate := resinSettingsFromParams(params, p.ResinConfig())
	normalized, errNormalize := normalizeResinSettings(candidate)
	if errNormalize != nil {
		return nil, http.StatusBadRequest, errNormalize
	}
	result, errValidate := validateResinSettings(ctx, normalized)
	if errValidate != nil {
		return nil, resinErrorStatus(errValidate), errValidate
	}
	return result, http.StatusOK, nil
}

func (p *Plugin) actionResinConfigSave(ctx context.Context, params map[string]any) (any, int, error) {
	store := p.stateStore()
	if store == nil {
		return nil, http.StatusInternalServerError, errors.New("state store is not initialized")
	}
	candidate := resinSettingsFromParams(params, p.ResinConfig())
	normalized, errNormalize := normalizeResinSettings(candidate)
	if errNormalize != nil {
		return nil, http.StatusBadRequest, errNormalize
	}
	result, errValidate := validateResinSettings(ctx, normalized)
	if errValidate != nil {
		return nil, resinErrorStatus(errValidate), errValidate
	}
	normalized.Platform = result.Platform
	normalized.Configured = true
	normalized.ValidatedAt = result.ValidatedAt
	if errStore := store.Update(func(state *State) error {
		state.Resin = normalized
		state.Policy.ResinAutoRotateAfterQualityFail = normalized.AutoRotateAfterQualityFailure
		p.recordEvent(state, "", "", "resin_config_save", "validated", "platform_id="+normalized.PlatformID)
		return nil
	}); errStore != nil {
		return nil, http.StatusInternalServerError, errStore
	}
	p.mu.Lock()
	p.policy.ResinAutoRotateAfterQualityFail = normalized.AutoRotateAfterQualityFailure
	p.cfg.Resin.Enabled = normalized.Enabled
	p.cfg.Resin.Platform = normalized.Platform
	p.cfg.Resin.PlatformID = normalized.PlatformID
	p.cfg.Resin.AdminURL = normalized.AdminURL
	p.cfg.Resin.ProxyTemplate = normalized.ProxyTemplate
	p.cfg.Resin.AutoRotateAfterQualityFailure = normalized.AutoRotateAfterQualityFailure
	p.mu.Unlock()
	return p.resinConfigView(normalized), http.StatusOK, nil
}

func resinSettingsFromParams(params map[string]any, current ResinSettings) ResinSettings {
	settings := current
	settings.Enabled = optionalBoolParam(params, "enabled", settings.Enabled)
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

func normalizeResinSettings(settings ResinSettings) (ResinSettings, error) {
	settings = normalizeResinSettingsBestEffort(settings)
	if settings.Platform == "" {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin platform is required")
	}
	if settings.PlatformID == "" {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin platform_id is required")
	}
	if !resinPlatformIDPattern.MatchString(settings.PlatformID) {
		return ResinSettings{}, resinConfigErrorf(http.StatusBadRequest, "resin platform_id must be a UUID")
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
	if errAdmin := validateResinAdminAPI(ctx, settings); errAdmin != nil {
		return result, errAdmin
	}
	result.AdminAPIReachable = true
	result.AdminTokenValid = true
	platformName, errPlatform := validateResinPlatform(ctx, settings)
	if errPlatform != nil {
		return result, errPlatform
	}
	settings.Platform = platformName
	result.Platform = platformName
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

func resolveResinPlatformForOperation(ctx context.Context, settings ResinSettings) (ResinSettings, error) {
	normalized, errNormalize := normalizeResinSettings(settings)
	if errNormalize != nil {
		return ResinSettings{}, errNormalize
	}
	platformName, errPlatform := validateResinPlatform(ctx, normalized)
	if errPlatform != nil {
		return ResinSettings{}, errPlatform
	}
	normalized.Platform = platformName
	return normalized, nil
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
	_ = conn.SetDeadline(deadline)
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

func resinErrorStatus(errValue error) int {
	var typed *resinConfigError
	if errors.As(errValue, &typed) && typed.Status > 0 {
		return typed.Status
	}
	return http.StatusBadRequest
}
