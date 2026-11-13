package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

const (
	StateUnknown            = "unknown"
	StateHealthy            = "healthy"
	StateSuspect            = "suspect"
	StateDegraded           = "degraded"
	StateCooling            = "cooling"
	StateQuotaExhausted     = "quota_exhausted"
	StateCredentialInvalid  = "credential_invalid"
	StateDisabledByGuard    = "disabled_by_guard"
	StateExternallyDisabled = "externally_disabled"
)

const (
	DisableKindCooldown          = "cooldown"
	DisableKindQuota             = "quota_exhausted"
	DisableKindCredentialInvalid = "credential_invalid"
	DisableKindPermanent         = "permanent"
)

const quotaExhaustedCooldown = 24 * time.Hour

const probeHeader = "X-Grok-Quality-Probe"

func isGrokProvider(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	return strings.Contains(normalized, "xai") || strings.Contains(normalized, "grok")
}

func isGrokModel(model string, prefixes []string) bool {
	normalized := strings.ToLower(strings.TrimSpace(model))
	if normalized == "" {
		return false
	}
	for _, prefix := range prefixes {
		prefix = strings.ToLower(strings.TrimSpace(prefix))
		if prefix != "" && strings.HasPrefix(normalized, prefix) {
			return true
		}
	}
	return strings.Contains(normalized, "grok")
}

// modelReasoningSupport infers reasoning capability from the model name instead
// of maintaining a table that must be updated for every model release. Grok 4+
// text models reason by default; explicit non-reasoning families and aliases
// override that default. Unknown families stay outside the thinking gate.
func modelReasoningSupport(model string) (known bool, supports bool) {
	slug := normalizeGrokModelSlug(model)
	if slug == "" {
		return false, false
	}
	if isKnownNonReasoningGrokModel(slug) {
		return true, false
	}
	if isReasoningDisabledAlias(slug) {
		return true, false
	}
	if major, ok := grokMajorVersion(slug); ok && major >= 4 {
		return true, true
	}
	// Grok 3 Mini is the only pre-4 family that emits reasoning tokens.
	if strings.HasPrefix(slug, "grok-3-mini") {
		return true, true
	}
	return false, false
}

func normalizeGrokModelSlug(model string) string {
	normalized := strings.ToLower(strings.TrimSpace(model))
	if idx := strings.LastIndexByte(normalized, '/'); idx >= 0 {
		normalized = strings.TrimSpace(normalized[idx+1:])
	}
	return normalized
}

func isKnownNonReasoningGrokModel(slug string) bool {
	switch {
	case strings.HasPrefix(slug, "grok-composer-"),
		strings.HasPrefix(slug, "grok-build-"),
		strings.HasPrefix(slug, "grok-imagine-"):
		return true
	}
	for _, marker := range []string{"non-reasoning", "non_reasoning", "nonreasoning", "no-reasoning"} {
		if strings.Contains(slug, marker) {
			return true
		}
	}
	return false
}

func isReasoningDisabledAlias(slug string) bool {
	return strings.HasSuffix(slug, "-none") || strings.HasSuffix(slug, "-off")
}

func grokMajorVersion(slug string) (int, bool) {
	const prefix = "grok-"
	if !strings.HasPrefix(slug, prefix) {
		return 0, false
	}
	rest := slug[len(prefix):]
	end := 0
	major := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		major = major*10 + int(rest[end]-'0')
		end++
	}
	if end == 0 {
		return 0, false
	}
	if end < len(rest) && rest[end] != '.' && rest[end] != '-' && rest[end] != '_' {
		return 0, false
	}
	return major, true
}

func isConfirmedClass(class string) bool {
	switch class {
	case ClassMissingThinking, ClassBufferedBurst, ClassCipherDrool, ClassFakeEncryptedDump, ClassFastReasoningRatio, ClassHardTPS, ClassEmptyStream, ClassExpectedMarkerMissing:
		return true
	default:
		return false
	}
}

func cooldownForClass(class string, cfg QualityConfig) time.Duration {
	switch class {
	case ClassEmptyStream:
		return cfg.EmptyStreamCooldown.Duration
	case ClassQuotaExhausted:
		return quotaExhaustedCooldown
	default:
		return cfg.MissingThinkingCooldown.Duration
	}
}

func authRawString(raw map[string]json.RawMessage, key string) string {
	if raw == nil {
		return ""
	}
	value := raw[key]
	if len(value) == 0 {
		return ""
	}
	var text string
	if errUnmarshal := json.Unmarshal(value, &text); errUnmarshal == nil {
		return text
	}
	return ""
}

func authRawBool(raw map[string]json.RawMessage, key string) bool {
	if raw == nil {
		return false
	}
	value := raw[key]
	if len(value) == 0 {
		return false
	}
	var boolean bool
	if errUnmarshal := json.Unmarshal(value, &boolean); errUnmarshal == nil {
		return boolean
	}
	return false
}

func authRawMap(raw map[string]json.RawMessage, key string) map[string]any {
	if raw == nil || len(raw[key]) == 0 {
		return nil
	}
	var value map[string]any
	if errUnmarshal := json.Unmarshal(raw[key], &value); errUnmarshal != nil {
		return nil
	}
	return value
}

func authEmail(raw map[string]json.RawMessage) string {
	if email := strings.TrimSpace(authRawString(raw, "email")); email != "" {
		return email
	}
	if meta := authRawMap(raw, "metadata"); meta != nil {
		if email := strings.TrimSpace(asString(meta["email"])); email != "" {
			return email
		}
	}
	return ""
}

func authLabel(raw map[string]json.RawMessage) string {
	for _, key := range []string{"label", "name", "account"} {
		if value := strings.TrimSpace(authRawString(raw, key)); value != "" {
			return value
		}
	}
	if meta := authRawMap(raw, "metadata"); meta != nil {
		for _, key := range []string{"label", "name", "account"} {
			if value := strings.TrimSpace(asString(meta[key])); value != "" {
				return value
			}
		}
	}
	return ""
}

func authDisabled(raw map[string]json.RawMessage) bool {
	return authRawBool(raw, "disabled")
}

func setRawBool(raw map[string]json.RawMessage, key string, value bool) {
	if raw == nil {
		return
	}
	encoded, _ := json.Marshal(value)
	raw[key] = encoded
}

func setRawString(raw map[string]json.RawMessage, key, value string) {
	if raw == nil {
		return
	}
	encoded, _ := json.Marshal(value)
	raw[key] = encoded
}

func authSubjectHash(raw map[string]json.RawMessage) string {
	seed := strings.Join([]string{
		authRawString(raw, "id"),
		authRawString(raw, "email"),
		authRawString(raw, "sub"),
		authRawString(raw, "subject"),
	}, "|")
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])[:8]
}

func isProbeHeader(headers map[string][]string) bool {
	if headers == nil {
		return false
	}
	for key, values := range headers {
		if !strings.EqualFold(key, probeHeader) {
			continue
		}
		for _, value := range values {
			if strings.TrimSpace(value) == "1" || strings.EqualFold(strings.TrimSpace(value), "true") {
				return true
			}
		}
	}
	return false
}

func isGrokAuth(raw map[string]json.RawMessage) bool {
	if raw == nil {
		return false
	}
	for _, key := range []string{"type", "provider", "oauth_provider", "base_url"} {
		if isGrokProvider(authRawString(raw, key)) {
			return true
		}
	}
	return false
}

func isCooldownState(state string) bool {
	return state == StateCooling || state == StateQuotaExhausted
}

func isIsolatedState(state string) bool {
	switch state {
	case StateCooling, StateQuotaExhausted, StateCredentialInvalid, StateDisabledByGuard, StateExternallyDisabled:
		return true
	default:
		return false
	}
}

func stateForDisableKind(kind string) string {
	switch kind {
	case DisableKindCooldown:
		return StateCooling
	case DisableKindQuota:
		return StateQuotaExhausted
	case DisableKindCredentialInvalid:
		return StateCredentialInvalid
	case DisableKindPermanent:
		return StateDisabledByGuard
	default:
		return StateCooling
	}
}

func isAccountStatusClass(class string) bool {
	return class == ClassCredentialInvalid || class == ClassQuotaExhausted
}
