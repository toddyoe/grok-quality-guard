package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDecodeConfigPartialKeepsDefaults(t *testing.T) {
	cfg, errDecode := decodeConfig([]byte("auto_action: cooldown_then_disable\n"))
	if errDecode != nil {
		t.Fatalf("decodeConfig() error = %v", errDecode)
	}
	if !cfg.PassiveGuard || cfg.TransparentGuard {
		t.Fatalf("unexpected guard defaults: passive=%v transparent=%v", cfg.PassiveGuard, cfg.TransparentGuard)
	}
	if cfg.AutoAction != "cooldown_then_disable" {
		t.Fatalf("auto_action = %q", cfg.AutoAction)
	}
	if cfg.Quality.SoftTPS != 500 || cfg.Quality.HardTPS != 1000 || cfg.Quality.MinGenerationMS != 1000 {
		t.Fatalf("unexpected quality defaults: %#v", cfg.Quality)
	}
	if cfg.Quality.HoldTimeout.Duration != 30*time.Second || cfg.Quality.MissingThinkingCooldown.Duration != 12*time.Hour {
		t.Fatalf("unexpected durations: %#v", cfg.Quality)
	}
}

func TestDecodeConfigResinLocalOnly(t *testing.T) {
	cfg, errDecode := decodeConfig([]byte("resin:\n  enabled: true\n  local_only: true\n"))
	if errDecode != nil {
		t.Fatalf("decodeConfig() error = %v", errDecode)
	}
	if !cfg.Resin.Enabled || !cfg.Resin.LocalOnly {
		t.Fatalf("unexpected local-only resin config: %#v", cfg.Resin)
	}
}
func TestNormalizePolicyRejectsInvalidTPS(t *testing.T) {
	policy := PolicyFromConfig(DefaultConfig())
	policy.Quality.SoftTPS = 1000
	policy.Quality.HardTPS = 500
	if _, errNormalize := normalizePolicy(policy); errNormalize == nil {
		t.Fatal("expected invalid TPS policy error")
	}
}

func TestConfigRevisionStable(t *testing.T) {
	firstRevision := configRevision([]byte("a: 1"))
	secondRevision := configRevision([]byte("a: 1"))
	if firstRevision != secondRevision {
		t.Fatal("same config should produce same revision")
	}
	if configRevision([]byte("a: 1")) == configRevision([]byte("a: 2")) {
		t.Fatal("different config should produce different revision")
	}
}

func TestDecodeConfigCooldownOnly(t *testing.T) {
	cfg, errDecode := decodeConfig([]byte("auto_action: cooldown_only\n"))
	if errDecode != nil {
		t.Fatalf("decodeConfig() error = %v", errDecode)
	}
	if cfg.AutoAction != "cooldown_only" {
		t.Fatalf("auto_action = %q", cfg.AutoAction)
	}
}

func TestProbeQueueConfigDefaultsAndBounds(t *testing.T) {
	cfg, errDecode := decodeConfig([]byte("probe:\n  concurrency: 2\n"))
	if errDecode != nil {
		t.Fatalf("decodeConfig() error = %v", errDecode)
	}
	if cfg.Probe.QueueSize != defaultProbeQueueSize {
		t.Fatalf("probe.queue_size = %d, want %d", cfg.Probe.QueueSize, defaultProbeQueueSize)
	}
	if cfg.Probe.UnknownJitter.Duration != defaultProbeUnknownJitter {
		t.Fatalf("probe.unknown_jitter = %s, want %s", cfg.Probe.UnknownJitter.Duration, defaultProbeUnknownJitter)
	}

	tooLarge := DefaultConfig()
	tooLarge.Probe.QueueSize = maxProbeQueueSize + 1
	if errNormalize := normalizeConfig(&tooLarge); errNormalize == nil {
		t.Fatal("expected oversized probe queue error")
	}
}
func TestDecodeConfigRoutingDefaults(t *testing.T) {
	cfg, errDecode := decodeConfig(nil)
	if errDecode != nil {
		t.Fatalf("decodeConfig() error = %v", errDecode)
	}
	if cfg.Routing.SelectionStrategy != "round_robin" || !cfg.Routing.PreferLastSuccess || !cfg.Routing.SessionAffinity {
		t.Fatalf("unexpected routing defaults: %#v", cfg.Routing)
	}
	if cfg.Routing.SessionAffinityTTL.Duration != time.Hour || cfg.Routing.SessionAffinityMaxSize != 10000 {
		t.Fatalf("unexpected routing affinity defaults: %#v", cfg.Routing)
	}
}

func TestDecodeConfigRoutingValidation(t *testing.T) {
	for _, raw := range []string{
		"routing:\n  selection_strategy: unsupported\n",
		"routing:\n  session_affinity_ttl: 169h\n",
		"routing:\n  session_affinity_max_entries: 100001\n",
	} {
		if _, errDecode := decodeConfig([]byte(raw)); errDecode == nil {
			t.Fatalf("expected routing config error for %q", raw)
		}
	}
}
func TestDecodeConfigPartialRoutingKeepsBooleanDefaults(t *testing.T) {
	cfg, errDecode := decodeConfig([]byte("routing:\n  selection_strategy: fill_first\n"))
	if errDecode != nil {
		t.Fatalf("decodeConfig() error = %v", errDecode)
	}
	if cfg.Routing.SelectionStrategy != "fill_first" || !cfg.Routing.PreferLastSuccess || !cfg.Routing.SessionAffinity {
		t.Fatalf("partial routing config lost defaults: %#v", cfg.Routing)
	}
}
func TestPolicyJSONWithoutRoutingUsesRoutingDefaults(t *testing.T) {
	var policy Policy
	if errUnmarshal := json.Unmarshal([]byte(`{"passive_guard":true,"auto_action":"observe"}`), &policy); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if policy.Routing.SelectionStrategy != "round_robin" || !policy.Routing.PreferLastSuccess || !policy.Routing.SessionAffinity {
		t.Fatalf("old state policy did not inherit routing defaults: %#v", policy.Routing)
	}
	if policy.Routing.SessionAffinityTTL.Duration != time.Hour || policy.Routing.SessionAffinityMaxSize != 10000 {
		t.Fatalf("old state policy did not inherit affinity defaults: %#v", policy.Routing)
	}
}
