package main

import (
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

const (
	pluginID   = "grok-quality-guard"
	pluginName = "Grok Quality Guard"
)

var pluginVersion = "0.1.0"

type registration struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Metadata      pluginapi.Metadata     `json:"metadata"`
	Capabilities  registrationCapability `json:"capabilities"`
}

type registrationCapability struct {
	ModelRouter            bool                         `json:"model_router"`
	Executor               bool                         `json:"executor"`
	ExecutorModelScope     pluginapi.ExecutorModelScope `json:"executor_model_scope"`
	ExecutorInputFormats   []string                     `json:"executor_input_formats,omitempty"`
	ExecutorOutputFormats  []string                     `json:"executor_output_formats,omitempty"`
	ResponseInterceptor    bool                         `json:"response_interceptor"`
	StreamChunkInterceptor bool                         `json:"response_stream_interceptor"`
	UsagePlugin            bool                         `json:"usage_plugin"`
	ManagementAPI          bool                         `json:"management_api"`
}

type managementRegistrationResponse struct {
	Routes    []pluginapi.ManagementRoute `json:"routes,omitempty"`
	Resources []pluginapi.ResourceRoute   `json:"resources,omitempty"`
}

func pluginRegistration() registration {
	return registration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name:             pluginName,
			Version:          pluginVersion,
			Author:           "grok-quality-guard contributors",
			GitHubRepository: "https://github.com/router-for-me/CLIProxyAPI",
			ConfigFields: []pluginapi.ConfigField{
				{Name: "passive_guard", Type: pluginapi.ConfigFieldTypeBoolean, Default: true, Description: "Observe request quality without changing routing."},
				{Name: "transparent_guard", Type: pluginapi.ConfigFieldTypeBoolean, Default: false, Description: "Hold and validate safe replayable Grok streams before emitting them."},
				{Name: "auto_action", Type: pluginapi.ConfigFieldTypeEnum, Default: "observe", EnumValues: []string{"observe", "cooldown_then_disable"}, Description: "Quality policy action for confirmed anomalies."},
				{Name: "model_prefixes", Type: pluginapi.ConfigFieldTypeArray, Default: []string{"grok-"}, Description: "Model prefixes considered Grok traffic."},
				{Name: "priority", Type: pluginapi.ConfigFieldTypeInteger, Default: 10, Description: "Plugin priority when multiple routing plugins are installed."},
				{Name: "quality.soft_tps", Type: pluginapi.ConfigFieldTypeNumber, Default: 500, Description: "Soft output tokens-per-second warning threshold."},
				{Name: "quality.hard_tps", Type: pluginapi.ConfigFieldTypeNumber, Default: 1000, Description: "Hard output tokens-per-second anomaly threshold."},
				{Name: "quality.min_generation_ms", Type: pluginapi.ConfigFieldTypeInteger, Default: 1000, Description: "Minimum generation window before a fast burst is considered anomalous."},
				{Name: "quality.min_passive_output_tokens", Type: pluginapi.ConfigFieldTypeInteger, Default: 32, Description: "Minimum output tokens for passive speed auditing."},
				{Name: "quality.min_hold_output_tokens", Type: pluginapi.ConfigFieldTypeInteger, Default: 8, Description: "Minimum visible output before transparent streaming release."},
				{Name: "quality.hold_timeout", Type: pluginapi.ConfigFieldTypeString, Default: "30s", Description: "Maximum transparent stream hold time."},
				{Name: "quality.hold_max_buffer_bytes", Type: pluginapi.ConfigFieldTypeInteger, Default: 4194304, Description: "Maximum transparent stream hold buffer."},
				{Name: "quality.min_encrypted_bytes", Type: pluginapi.ConfigFieldTypeInteger, Default: 256, Description: "Minimum encrypted reasoning bytes."},
				{Name: "quality.encrypted_bytes_per_reasoning_token", Type: pluginapi.ConfigFieldTypeInteger, Default: 4, Description: "Required encrypted bytes per billed reasoning token."},
				{Name: "quality.missing_thinking_cooldown", Type: pluginapi.ConfigFieldTypeString, Default: "12h", Description: "Cooldown after the first confirmed missing-thinking anomaly."},
				{Name: "quality.empty_stream_cooldown", Type: pluginapi.ConfigFieldTypeString, Default: "15m", Description: "Cooldown after an empty stream."},
				{Name: "quality.max_attempts", Type: pluginapi.ConfigFieldTypeInteger, Default: 6, Description: "Maximum transparent upstream attempts per request."},
				{Name: "quality.on_exhausted", Type: pluginapi.ConfigFieldTypeEnum, Default: "fail_closed", EnumValues: []string{"fail_closed", "fail_open"}, Description: "Behavior after every transparent candidate fails the quality gate."},
				{Name: "probe.recovery_only", Type: pluginapi.ConfigFieldTypeBoolean, Default: true, Description: "Probe only unknown, suspect, and cooling accounts."},
				{Name: "probe.model", Type: pluginapi.ConfigFieldTypeString, Default: "grok-4.5", Description: "Model used by active recovery probes."},
				{Name: "probe.timeout", Type: pluginapi.ConfigFieldTypeString, Default: "90s", Description: "Active probe timeout."},
				{Name: "probe.max_output_tokens", Type: pluginapi.ConfigFieldTypeInteger, Default: 768, Description: "Active probe max output tokens."},
				{Name: "probe.concurrency", Type: pluginapi.ConfigFieldTypeInteger, Default: 1, Description: "Active probe concurrency."},
				{Name: "probe.unknown_delay", Type: pluginapi.ConfigFieldTypeString, Default: "30s", Description: "Delay before the first probe of a new account."},
				{Name: "probe.suspect_backoff", Type: pluginapi.ConfigFieldTypeString, Default: "15m", Description: "Initial suspect probe backoff."},
				{Name: "probe.max_backoff", Type: pluginapi.ConfigFieldTypeString, Default: "1h", Description: "Maximum suspect probe backoff."},
				{Name: "state_file", Type: pluginapi.ConfigFieldTypeString, Description: "Optional state file path override."},
				{Name: "resin.enabled", Type: pluginapi.ConfigFieldTypeBoolean, Default: false, Description: "Enable Resin account lease management."},
				{Name: "resin.platform", Type: pluginapi.ConfigFieldTypeString, Default: "Default", Description: "Resin platform name used in proxy URLs."},
				{Name: "resin.platform_id", Type: pluginapi.ConfigFieldTypeString, Description: "Resin platform UUID used by lease deletion."},
				{Name: "resin.admin_url", Type: pluginapi.ConfigFieldTypeString, Description: "Resin admin API base URL."},
				{Name: "resin.admin_token_env", Type: pluginapi.ConfigFieldTypeString, Default: "RESIN_ADMIN_TOKEN", Description: "Environment variable for the Resin admin token."},
				{Name: "resin.proxy_token_env", Type: pluginapi.ConfigFieldTypeString, Default: "RESIN_PROXY_TOKEN", Description: "Environment variable for the Resin proxy token."},
				{Name: "resin.proxy_template", Type: pluginapi.ConfigFieldTypeString, Default: "socks5h://{platform}.{account}:{proxy_token}@resin:2260", Description: "Resin proxy URL template."},
				{Name: "resin.auto_rotate_after_quality_failure", Type: pluginapi.ConfigFieldTypeBoolean, Default: true, Description: "Rotate a managed Resin lease after a confirmed quality failure."},
				{Name: "events_limit", Type: pluginapi.ConfigFieldTypeInteger, Default: 1000, Description: "Maximum retained quality events."},
			},
		},
		Capabilities: registrationCapability{
			ModelRouter:            true,
			Executor:               true,
			ExecutorModelScope:     pluginapi.ExecutorModelScopeBoth,
			ExecutorInputFormats:   []string{"codex"},
			ExecutorOutputFormats:  []string{"codex"},
			ResponseInterceptor:    true,
			StreamChunkInterceptor: true,
			UsagePlugin:            true,
			ManagementAPI:          true,
		},
	}
}

func (p *Plugin) ManagementRegistration() managementRegistrationResponse {
	return managementRegistrationResponse{
		Routes: []pluginapi.ManagementRoute{{
			Method: "POST",
			Path:   "/plugins/" + pluginID + "/api",
		}},
		Resources: []pluginapi.ResourceRoute{{
			Path:        "/dashboard",
			Menu:        pluginName,
			Description: "Grok account quality, Resin leases, probes, and policy controls.",
		}},
	}
}
