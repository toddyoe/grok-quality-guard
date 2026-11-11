package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	StateUnknown            = "unknown"
	StateHealthy            = "healthy"
	StateSuspect            = "suspect"
	StateDegraded           = "degraded"
	StateCooling            = "cooling"
	StateDisabledByGuard    = "disabled_by_guard"
	StateExternallyDisabled = "externally_disabled"
)

const (
	DisableKindCooldown  = "cooldown"
	DisableKindPermanent = "permanent"
)

const probeHeader = "X-Grok-Quality-Probe"

type GuardMeta struct {
	Managed               bool   `json:"managed,omitempty"`
	ResinAccount          string `json:"resin_account,omitempty"`
	ResinGeneration       int64  `json:"resin_generation,omitempty"`
	DisableKind           string `json:"disable_kind,omitempty"`
	CooldownUntil         string `json:"cooldown_until,omitempty"`
	PreviousDisabled      bool   `json:"previous_disabled,omitempty"`
	StrikeMissingThinking bool   `json:"strike_missing_thinking,omitempty"`
}

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

func isConfirmedClass(class string) bool {
	switch class {
	case ClassMissingThinking, ClassBufferedBurst, ClassCipherDrool, ClassFakeEncryptedDump, ClassFastReasoningRatio, ClassHardTPS, ClassEmptyStream:
		return true
	default:
		return false
	}
}

func cooldownForClass(class string, cfg QualityConfig) time.Duration {
	if class == ClassEmptyStream {
		return cfg.EmptyStreamCooldown.Duration
	}
	return cfg.MissingThinkingCooldown.Duration
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

func guardMeta(raw map[string]json.RawMessage) GuardMeta {
	value := authRawMap(raw, "grok_quality_guard")
	if value == nil {
		return GuardMeta{}
	}
	var meta GuardMeta
	encoded, _ := json.Marshal(value)
	_ = json.Unmarshal(encoded, &meta)
	return meta
}

func setGuardMeta(raw map[string]json.RawMessage, meta GuardMeta) error {
	if raw == nil {
		return fmt.Errorf("auth payload is nil")
	}
	encoded, errMarshal := json.Marshal(meta)
	if errMarshal != nil {
		return errMarshal
	}
	raw["grok_quality_guard"] = encoded
	return nil
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

func authDisabledState(raw map[string]json.RawMessage) string {
	meta := guardMeta(raw)
	if authDisabled(raw) {
		switch meta.DisableKind {
		case DisableKindCooldown:
			return StateCooling
		case DisableKindPermanent:
			return StateDisabledByGuard
		default:
			return StateExternallyDisabled
		}
	}
	return ""
}
