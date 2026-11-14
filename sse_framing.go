package main

import (
	"bytes"
	"encoding/json"
)

// sseOutputNormalizer turns CPA line-oriented executor chunks into complete SSE
// events. Plugin host adapters expect each emitted payload to contain one event,
// so event and data lines are paired before they reach the host stream bridge.
type sseOutputNormalizer struct {
	pending []byte
	prefix  []byte
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
		if normalized := n.normalizeLine(line); len(normalized) > 0 {
			output = append(output, normalized)
		}
	}
	if len(n.pending) > 0 && sseOutputLineComplete(n.pending) {
		line := bytes.TrimSuffix(n.pending, []byte{'\r'})
		n.pending = nil
		if normalized := n.normalizeLine(line); len(normalized) > 0 {
			output = append(output, normalized)
		}
	}
	return output
}

func (n *sseOutputNormalizer) Flush() [][]byte {
	if n == nil {
		return nil
	}
	var output [][]byte
	if len(n.pending) > 0 {
		pending := bytes.Clone(n.pending)
		n.pending = nil
		if sseOutputLineComplete(pending) {
			if normalized := n.normalizeLine(bytes.TrimSuffix(pending, []byte{'\r'})); len(normalized) > 0 {
				output = append(output, normalized)
			}
		}
	}
	n.prefix = nil
	return output
}

func (n *sseOutputNormalizer) normalizeLine(line []byte) []byte {
	line = bytes.TrimSuffix(line, []byte{'\r'})
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 {
		return nil
	}
	fieldLine := append([]byte(nil), line...)
	switch {
	case bytes.HasPrefix(trimmed, []byte("data:")):
		frame := append([]byte(nil), n.prefix...)
		frame = append(frame, fieldLine...)
		n.prefix = nil
		return append(frame, '\n', '\n')
	case bytes.HasPrefix(trimmed, []byte("event:")) || bytes.HasPrefix(trimmed, []byte("id:")) ||
		bytes.HasPrefix(trimmed, []byte("retry:")):
		n.prefix = append(n.prefix, fieldLine...)
		n.prefix = append(n.prefix, '\n')
		return nil
	case bytes.HasPrefix(trimmed, []byte(":")):
		if len(n.prefix) > 0 {
			n.prefix = append(n.prefix, fieldLine...)
			n.prefix = append(n.prefix, '\n')
			return nil
		}
		return append(fieldLine, '\n', '\n')
	case bytes.Equal(trimmed, []byte("[DONE]")):
		return append(fieldLine, '\n', '\n')
	default:
		if len(n.prefix) > 0 {
			n.prefix = append(n.prefix, fieldLine...)
			n.prefix = append(n.prefix, '\n')
			return nil
		}
		return append(fieldLine, '\n')
	}
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
