package main

import (
	"bytes"
	"encoding/json"
)

// sseOutputNormalizer turns CPA line-oriented executor chunks into complete SSE
// field lines. The plugin host assembles events from blank-line boundaries, so
// every data line must carry the terminating blank line itself.
type sseOutputNormalizer struct {
	pending []byte
}

func (n *sseOutputNormalizer) Feed(chunk []byte) [][]byte {
	if n == nil || len(chunk) == 0 {
		return nil
	}
	if len(n.pending) == 0 && !sseOutputCanStart(chunk) {
		return [][]byte{bytes.Clone(chunk)}
	}
	n.pending = append(n.pending, chunk...)
	var output [][]byte
	for {
		idx := bytes.IndexByte(n.pending, '\n')
		if idx < 0 {
			break
		}
		line := bytes.TrimSuffix(n.pending[:idx], []byte{'\r'})
		n.pending = n.pending[idx+1:]
		if normalized := normalizeSSEOutputLine(line); len(normalized) > 0 {
			output = append(output, normalized)
		}
	}
	if len(n.pending) > 0 && sseOutputLineComplete(n.pending) {
		line := bytes.TrimSuffix(n.pending, []byte{'\r'})
		n.pending = nil
		if normalized := normalizeSSEOutputLine(line); len(normalized) > 0 {
			output = append(output, normalized)
		}
	}
	return output
}

func (n *sseOutputNormalizer) Flush() [][]byte {
	if n == nil || len(n.pending) == 0 {
		return nil
	}
	pending := bytes.Clone(n.pending)
	n.pending = nil
	if normalized := normalizeSSEOutputLine(pending); len(normalized) > 0 {
		return [][]byte{normalized}
	}
	return nil
}

func sseOutputCanStart(chunk []byte) bool {
	trimmed := bytes.TrimLeft(chunk, " \t")
	if bytes.HasPrefix(trimmed, []byte(":")) {
		return true
	}
	for _, field := range [][]byte{[]byte("data:"), []byte("event:"), []byte("id:"), []byte("retry:")} {
		if bytes.HasPrefix(trimmed, field) {
			return true
		}
		if len(trimmed) > 0 && len(trimmed) < len(field) && bytes.HasPrefix(field, trimmed) {
			return true
		}
	}
	return false
}

func sseOutputLineComplete(line []byte) bool {
	line = bytes.TrimSuffix(line, []byte{'\r'})
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 {
		return true
	}
	if bytes.HasPrefix(trimmed, []byte("event:")) || bytes.HasPrefix(trimmed, []byte("id:")) ||
		bytes.HasPrefix(trimmed, []byte("retry:")) || bytes.HasPrefix(trimmed, []byte(":")) {
		return true
	}
	if data, ok := bytes.CutPrefix(trimmed, []byte("data:")); ok {
		data = bytes.TrimSpace(data)
		return len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) || json.Valid(data)
	}
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		return true
	}
	if trimmed[0] == '{' || trimmed[0] == '[' {
		return json.Valid(trimmed)
	}
	return false
}

func normalizeSSEOutputLine(line []byte) []byte {
	line = bytes.TrimSuffix(line, []byte{'\r'})
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 {
		return nil
	}
	output := append([]byte(nil), line...)
	switch {
	case bytes.HasPrefix(trimmed, []byte("data:")):
		return append(output, '\n', '\n')
	case bytes.HasPrefix(trimmed, []byte("event:")) || bytes.HasPrefix(trimmed, []byte("id:")) ||
		bytes.HasPrefix(trimmed, []byte("retry:")):
		return append(output, '\n')
	case bytes.HasPrefix(trimmed, []byte(":")):
		return append(output, '\n', '\n')
	case bytes.Equal(trimmed, []byte("[DONE]")):
		return append(output, '\n', '\n')
	default:
		return append(output, '\n')
	}
}
