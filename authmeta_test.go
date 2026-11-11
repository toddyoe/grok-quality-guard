package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGuardMetadataAndAuthIdentity(t *testing.T) {
	raw := map[string]json.RawMessage{}
	for key, value := range map[string]any{
		"type":          "xai",
		"email":         "Alice@Example.com",
		"access_token":  "access",
		"refresh_token": "refresh",
		"disabled":      true,
		"proxy_url":     "socks5h://Default.grok-alice-1abc:secret@resin:2260",
		"grok_quality_guard": map[string]any{
			"managed":                 true,
			"resin_account":           "grok-alice-1abc",
			"disable_kind":            "cooldown",
			"cooldown_until":          "2026-11-11T13:00:00Z",
			"strike_missing_thinking": true,
		},
	} {
		encoded, _ := json.Marshal(value)
		raw[key] = encoded
	}
	if !isGrokAuth(raw) {
		t.Fatal("xai auth not recognized")
	}
	meta := guardMeta(raw)
	if !meta.Managed || meta.ResinAccount != "grok-alice-1abc" || meta.DisableKind != DisableKindCooldown || !meta.StrikeMissingThinking {
		t.Fatalf("unexpected guard metadata: %#v", meta)
	}
	if state := authDisabledState(raw); state != StateCooling {
		t.Fatalf("disabled state = %q, want cooling", state)
	}
	if base := resinAccountBase(raw); base != "alice" {
		t.Fatalf("resin base = %q, want alice", base)
	}
	if mask := maskEmail("alice@example.com"); mask != "al***e@example.com" {
		t.Fatalf("masked email = %q", mask)
	}
	if !strings.Contains(maskProxyURL("socks5h://user:pass@example.com:1080/path"), "socks5h://example.com:1080") {
		t.Fatal("proxy mask did not preserve scheme and host")
	}
}

func TestResinAccountBaseFallbacks(t *testing.T) {
	raw := map[string]json.RawMessage{}
	encoded, _ := json.Marshal("label with Spaces!!")
	raw["label"] = encoded
	if got := resinAccountBase(raw); got != "label_with_spaces" {
		t.Fatalf("resin base = %q", got)
	}
	raw = map[string]json.RawMessage{}
	encoded, _ = json.Marshal("subject-only")
	raw["sub"] = encoded
	if got := resinAccountBase(raw); !strings.HasPrefix(got, "auth_") {
		t.Fatalf("fallback base = %q", got)
	}
}

func TestSafeReplayRequest(t *testing.T) {
	safe := []byte(`{"model":"grok-4.5","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	if !safeReplayRequest(safe, "codex", "grok-4.5") {
		t.Fatal("simple text request should be safe")
	}
	unsafeCases := [][]byte{
		[]byte(`{"previous_response_id":"resp_1"}`),
		[]byte(`{"store":true}`),
		[]byte(`{"background":true}`),
		[]byte(`{"web_search_options":{}}`),
		[]byte(`{"tools":[{"type":"web_search"}]}`),
		[]byte(`{"reasoning_effort":"none"}`),
		[]byte(`{"input":[{"type":"compaction_trigger"}]}`),
		[]byte(`{"input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}]}`), []byte(`{"modalities":["text","image"],"messages":[]}`),
	}
	for _, body := range unsafeCases {
		if safeReplayRequest(body, "codex", "grok-4.5") {
			t.Fatalf("request should be unsafe: %s", body)
		}
	}
}
