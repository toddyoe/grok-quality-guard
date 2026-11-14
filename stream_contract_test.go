package main

import (
	"testing"
)

// TestRegistrationDeclaresClientOutputFormats guards the executor format contract.
// The plugin re-emits upstream Responses SSE frames unchanged, so every client
// protocol it serves must be declared as an output format. An undeclared protocol
// is served through host-side translation, where an unchanged frame is treated as
// an SDK fallback and dropped: the client then receives no payload at all and the
// handler closes the request with a 502 before the first data frame.
func TestRegistrationDeclaresClientOutputFormats(t *testing.T) {
	formats := pluginRegistration().Capabilities.ExecutorOutputFormats
	declared := make(map[string]bool, len(formats))
	for _, format := range formats {
		declared[format] = true
	}
	for _, required := range []string{"codex", "openai-response"} {
		if !declared[required] {
			t.Fatalf("executor output formats = %v, want %q declared; undeclared protocols drop every emitted frame", formats, required)
		}
	}
}

// TestTransparentExecutorUsesPayloadFormatAndTargetExitProtocol keeps the host
// callback protocols distinct: SourceFormat describes the translated payload while
// Format describes the response the host expects the plugin to return.
func TestTransparentExecutorUsesPayloadFormatAndTargetExitProtocol(t *testing.T) {
	req := responsesRequestForTest("grok-4.6", true)
	req.Payload = []byte(`{"model":"grok-4.6","stream":true,"input":"hi"}`)
	req.SourceFormat = "codex"
	req.Format = "openai-response"

	_, sourceFormat, errRequest := executorRequestInput(req)
	if errRequest != nil {
		t.Fatal(errRequest)
	}
	if sourceFormat != "codex" {
		t.Fatalf("payload source format = %q, want codex", sourceFormat)
	}

	execution := buildHostExecution(req, req.Payload, sourceFormat, "auth-1", "", true)
	if execution.EntryProtocol != "codex" || execution.ExitProtocol != "openai-response" {
		t.Fatalf("execution protocols = entry %q, exit %q; want codex -> openai-response", execution.EntryProtocol, execution.ExitProtocol)
	}
}
