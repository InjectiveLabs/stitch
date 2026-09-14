package cache

import (
	"bytes"
	"encoding/json"
)

// IsSuccessfulResponse reports whether body is a JSON-RPC success envelope
// with a result and ID. HTTP 200 alone is insufficient: transient JSON-RPC
// errors must not become shared responses for every caller of a cache key.
func IsSuccessfulResponse(body []byte) bool {
	_, _, ok := responseIDRange(body)
	return ok
}

// ResponseWithID returns a successful cached JSON-RPC response with the
// caller's ID. Only the top-level ID's bytes are replaced; result values,
// nested IDs, large numbers, and all other response bytes remain intact.
// It returns false for notifications, invalid IDs, or invalid envelopes.
// The input body is never modified. An unchanged body may be returned as-is.
func ResponseWithID(body []byte, id json.RawMessage) ([]byte, bool) {
	if !IsJSONRPCID(id) {
		return nil, false
	}
	start, end, ok := responseIDRange(body)
	if !ok {
		return nil, false
	}
	if bytes.Equal(body[start:end], id) {
		return body, true
	}
	out := make([]byte, 0, len(body)-(end-start)+len(id))
	out = append(out, body[:start]...)
	out = append(out, id...)
	out = append(out, body[end:]...)
	return out, true
}

// responseIDRange validates the envelope without copying or decoding its
// potentially large result. After json.Valid verifies the syntax, scanning
// top-level value boundaries is enough to inspect the small envelope fields
// and locate the exact ID token, even when its key is escaped in JSON.
func responseIDRange(body []byte) (start, end int, ok bool) {
	if !json.Valid(body) {
		return 0, 0, false
	}
	pos := skipJSONSpace(body, 0)
	if body[pos] != '{' {
		return 0, 0, false
	}
	seen := make(map[string]bool)
	for pos = skipJSONSpace(body, pos+1); body[pos] != '}'; {
		keyEnd := jsonStringEnd(body, pos)
		var name string
		if err := json.Unmarshal(body[pos:keyEnd], &name); err != nil || seen[name] {
			return 0, 0, false
		}
		seen[name] = true
		// The validated object guarantees a colon after its key.
		pos = skipJSONSpace(body, skipJSONSpace(body, keyEnd)+1)
		valueEnd := jsonValueEnd(body, pos)
		value := body[pos:valueEnd]
		switch name {
		case "id":
			if !IsJSONRPCID(value) {
				return 0, 0, false
			}
			start, end = pos, valueEnd
		case "error":
			if !bytes.Equal(value, []byte("null")) {
				return 0, 0, false
			}
		case "jsonrpc":
			var version string
			if err := json.Unmarshal(value, &version); err != nil || version != "2.0" {
				return 0, 0, false
			}
		}
		pos = skipJSONSpace(body, valueEnd)
		if body[pos] == ',' {
			pos = skipJSONSpace(body, pos+1)
		}
	}
	return start, end, seen["jsonrpc"] && seen["result"] && seen["id"]
}

// These boundary scanners are only called after json.Valid. They do not
// parse JSON syntax; they skip known-valid strings, containers and scalars
// while retaining offsets into the original body.
func skipJSONSpace(body []byte, pos int) int {
	for pos < len(body) {
		switch body[pos] {
		case ' ', '\t', '\r', '\n':
			pos++
		default:
			return pos
		}
	}
	return pos
}

func jsonStringEnd(body []byte, pos int) int {
	for pos++; ; pos++ {
		switch body[pos] {
		case '\\':
			pos++ // Skip the escaped character, including escaped quotes.
		case '"':
			return pos + 1
		}
	}
}

func jsonValueEnd(body []byte, pos int) int {
	switch body[pos] {
	case '"':
		return jsonStringEnd(body, pos)
	case '{', '[':
		depth := 1
		for pos++; ; pos++ {
			switch body[pos] {
			case '"':
				pos = jsonStringEnd(body, pos) - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return pos + 1
				}
			}
		}
	default:
		for ; ; pos++ {
			switch body[pos] {
			case ',', '}', ']', ' ', '\t', '\r', '\n':
				return pos
			}
		}
	}
}

// IsJSONRPCID reports whether id is an explicit JSON-RPC string, number,
// or null ID. A missing ID is a notification and must bypass the cache.
func IsJSONRPCID(id json.RawMessage) bool {
	id = bytes.TrimSpace(id)
	if len(id) == 0 || !json.Valid(id) {
		return false
	}
	return id[0] == '"' || id[0] == '-' || (id[0] >= '0' && id[0] <= '9') || bytes.Equal(id, []byte("null"))
}
