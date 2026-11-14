package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

const (
	maxSessionSeedBytes      = 1024
	maxCodexTurnMetadataSize = 16 << 10
)

func (p *Plugin) selectionSessionKey(req rpcExecutorRequest, body []byte, sourceFormat string) string {
	seed := extractSelectionSessionSeed(req.Headers, body)
	if seed == "" {
		return ""
	}
	callerScope := strings.TrimSpace(asString(req.Metadata[pluginapi.CallerScopeMetadataKey]))
	if callerScope == "" {
		callerScope = callerScopeFromHeaders(req.Headers)
	}
	model := strings.ToLower(strings.TrimSpace(executionModel(req, body)))
	source := "grok-quality-guard:affinity:v1\x00" +
		callerScope + "\x00" +
		"xai\x00" +
		model + "\x00" +
		strings.ToLower(strings.TrimSpace(sourceFormat)) + "\x00" +
		seed
	digest := sha256.Sum256([]byte(source))
	return hex.EncodeToString(digest[:])
}

func callerScopeFromHeaders(headers http.Header) string {
	if headers == nil {
		return ""
	}
	value := firstNonEmpty(headers.Get("Authorization"), headers.Get("X-Api-Key"), headers.Get("X-Goog-Api-Key"))
	if value == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(value))
	return "auth:" + hex.EncodeToString(digest[:])
}

func extractSelectionSessionSeed(headers http.Header, body []byte) string {
	if headers != nil {
		if seed := normalizeSessionSeed(headers.Get("X-Claude-Code-Session-Id")); seed != "" {
			return seed
		}
		if seed := codexSessionSeedFromHeaders(headers); seed != "" {
			return seed
		}
		for _, name := range []string{
			"X-Session-Id", "Session-Id", "Session_id",
			"X-Conversation-Id", "Conversation-Id", "Conversation_id",
			"X-Client-Session-Id", "X-Grok-Conv-Id",
		} {
			if seed := normalizeSessionSeed(headers.Get(name)); seed != "" {
				return seed
			}
		}
	}
	var payload struct {
		PromptCacheKey      string          `json:"prompt_cache_key"`
		ConversationID      string          `json:"conversation_id"`
		ConversationIDCamel string          `json:"conversationId"`
		Conversation        json.RawMessage `json:"conversation"`
		SessionID           string          `json:"session_id"`
		SessionIDCamel      string          `json:"sessionId"`
		Metadata            struct {
			SessionID      string `json:"session_id"`
			SessionIDCamel string `json:"sessionId"`
		} `json:"metadata"`
		ClientMetadata map[string]json.RawMessage `json:"client_metadata"`
	}
	if len(body) > 0 && json.Unmarshal(body, &payload) == nil {
		if seed := normalizeSessionSeed(payload.PromptCacheKey); seed != "" {
			return seed
		}
		if seed := normalizeSessionSeed(payload.Metadata.SessionID); seed != "" {
			return seed
		}
		if seed := normalizeSessionSeed(payload.Metadata.SessionIDCamel); seed != "" {
			return seed
		}
		if seed := codexSessionSeedFromRawMetadata(payload.ClientMetadata["x-codex-turn-metadata"]); seed != "" {
			return seed
		}
		if seed := normalizeRawSessionSeed(payload.ClientMetadata["x-codex-window-id"]); seed != "" {
			return "codex:window:" + seed
		}
		if seed := normalizeSessionSeed(payload.SessionID); seed != "" {
			return seed
		}
		if seed := normalizeSessionSeed(payload.SessionIDCamel); seed != "" {
			return seed
		}
		if seed := normalizeSessionSeed(payload.ConversationID); seed != "" {
			return seed
		}
		if seed := normalizeSessionSeed(payload.ConversationIDCamel); seed != "" {
			return seed
		}
		if seed := conversationSessionSeed(payload.Conversation); seed != "" {
			return seed
		}
	}
	return ""
}

func conversationSessionSeed(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return normalizeSessionSeed(text)
	}
	var object struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(raw, &object) == nil {
		return normalizeSessionSeed(object.ID)
	}
	return ""
}

func codexSessionSeedFromHeaders(headers http.Header) string {
	if headers == nil {
		return ""
	}
	if seed := codexSessionSeedFromTurnMetadata(headers.Get("X-Codex-Turn-Metadata")); seed != "" {
		return seed
	}
	if seed := normalizeSessionSeed(headers.Get("X-Codex-Window-Id")); seed != "" {
		return "codex:window:" + seed
	}
	return ""
}

func codexSessionSeedFromTurnMetadata(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxCodexTurnMetadataSize {
		return ""
	}
	var metadata struct {
		PromptCacheKey string `json:"prompt_cache_key"`
		WindowID       string `json:"window_id"`
	}
	if json.Unmarshal([]byte(value), &metadata) != nil {
		return ""
	}
	if seed := normalizeSessionSeed(metadata.PromptCacheKey); seed != "" {
		return seed
	}
	if seed := normalizeSessionSeed(metadata.WindowID); seed != "" {
		return "codex:window:" + seed
	}
	return ""
}

func codexSessionSeedFromRawMetadata(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return codexSessionSeedFromTurnMetadata(value)
	}
	return codexSessionSeedFromTurnMetadata(string(raw))
}

func normalizeRawSessionSeed(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return normalizeSessionSeed(value)
}

func normalizeSessionSeed(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxSessionSeedBytes {
		return ""
	}
	return value
}
