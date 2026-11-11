package main

import (
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
	if configRevision([]byte("a: 1")) != configRevision([]byte("a: 1")) {
		t.Fatal("same config should produce same revision")
	}
	if configRevision([]byte("a: 1")) == configRevision([]byte("a: 2")) {
		t.Fatal("different config should produce different revision")
	}
}
