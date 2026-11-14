package main

import (
	"encoding/json"
	"os"
	"testing"
)

func TestPluginLogoConfiguration(t *testing.T) {
	registration := pluginRegistration()
	if registration.Metadata.Logo != pluginLogo {
		t.Fatalf("registration logo = %q, want %q", registration.Metadata.Logo, pluginLogo)
	}
	if info, errStat := os.Stat("assets/logo.png"); errStat != nil {
		t.Fatalf("logo asset is missing: %v", errStat)
	} else if info.Size() == 0 {
		t.Fatal("logo asset is empty")
	}
	rawRegistry, errRead := os.ReadFile("registry.json")
	if errRead != nil {
		t.Fatalf("read registry: %v", errRead)
	}
	var registry struct {
		Plugins []struct {
			ID   string `json:"id"`
			Logo string `json:"logo"`
		} `json:"plugins"`
	}
	if errUnmarshal := json.Unmarshal(rawRegistry, &registry); errUnmarshal != nil {
		t.Fatalf("decode registry: %v", errUnmarshal)
	}
	for _, plugin := range registry.Plugins {
		if plugin.ID == pluginID {
			if plugin.Logo != pluginLogo {
				t.Fatalf("registry logo = %q, want %q", plugin.Logo, pluginLogo)
			}
			return
		}
	}
	t.Fatalf("registry does not contain plugin %q", pluginID)
}
func TestRegistrationIncludesRoutingSelectionConfig(t *testing.T) {
	registration := pluginRegistration()
	fields := make(map[string]bool, len(registration.Metadata.ConfigFields))
	for _, field := range registration.Metadata.ConfigFields {
		fields[field.Name] = true
	}
	for _, name := range []string{
		"routing.selection_strategy",
		"routing.prefer_last_success",
		"routing.session_affinity",
		"routing.session_affinity_ttl",
		"routing.session_affinity_max_entries",
	} {
		if !fields[name] {
			t.Fatalf("registration is missing config field %q", name)
		}
	}
}
