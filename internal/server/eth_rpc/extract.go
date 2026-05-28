package eth_rpc

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// param returns the i-th element of a JSON-RPC params array, or nil if
// params is not an array or i is out of range.
func param(raw json.RawMessage, i int) json.RawMessage {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '[' {
		return nil
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil
	}
	if i < 0 || i >= len(arr) {
		return nil
	}
	return arr[i]
}

// parseHexUint64 parses an EVM-style hex string ("0x10") into uint64. Tags
// like "latest"/"earliest"/"pending"/"safe"/"finalized" return (0, false).
func parseHexUint64(s string) (int64, bool) {
	s = strings.TrimSpace(strings.Trim(s, `"`))
	if s == "" {
		return 0, false
	}
	switch s {
	case "earliest":
		return 1, true
	case "latest", "pending", "safe", "finalized":
		return 0, false
	}
	if !strings.HasPrefix(s, "0x") && !strings.HasPrefix(s, "0X") {
		// Some clients still send decimal; tolerate it.
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v, true
		}
		return 0, false
	}
	v, err := strconv.ParseInt(s[2:], 16, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// extractBlockNumber pulls a block-number-shaped value from a JSON-RPC
// params element. The element may be a hex string or an object form like
// {"blockNumber":"0x..."}; we accept either.
func extractBlockNumber(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	raw = bytes.TrimSpace(raw)
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return parseHexUint64(s)
		}
		return 0, false
	}
	if raw[0] == '{' {
		// BlockNumberOrHash object — check blockNumber first.
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return 0, false
		}
		if v, ok := obj["blockNumber"]; ok {
			var s string
			if err := json.Unmarshal(v, &s); err == nil {
				return parseHexUint64(s)
			}
		}
		return 0, false
	}
	return 0, false
}

// extractBlockHash pulls a block-hash from a params element. Accepts
// either a hex string or an object form {"blockHash":"0x..."}.
func extractBlockHash(raw json.RawMessage) ([]byte, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	raw = bytes.TrimSpace(raw)
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil || !looksLikeHash(s) {
			return nil, false
		}
		return []byte(s), true
	}
	if raw[0] == '{' {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, false
		}
		if v, ok := obj["blockHash"]; ok {
			var s string
			if err := json.Unmarshal(v, &s); err == nil && looksLikeHash(s) {
				return []byte(s), true
			}
		}
	}
	return nil, false
}

// extractTxHash pulls a tx hash (always a quoted hex string).
func extractTxHash(raw json.RawMessage) ([]byte, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var s string
	if err := json.Unmarshal(bytes.TrimSpace(raw), &s); err != nil || !looksLikeHash(s) {
		return nil, false
	}
	return []byte(s), true
}

// extractFilterID pulls an eth_filter id ("0x..." quantity) from a param
// element. Returns the raw hex string for use as a sticky-filter map key.
func extractFilterID(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var s string
	if err := json.Unmarshal(bytes.TrimSpace(raw), &s); err != nil {
		return "", false
	}
	if !strings.HasPrefix(s, "0x") || len(s) < 3 {
		return "", false
	}
	return s, true
}

// looksLikeHash returns true for "0x" + 64 hex digits. Stricter than what
// EVM clients sometimes send (which may pad zeros) but enough for routing
// decisions.
func looksLikeHash(s string) bool {
	if !strings.HasPrefix(s, "0x") || len(s) < 3 {
		return false
	}
	for _, c := range s[2:] {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}
