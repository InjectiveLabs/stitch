package cache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/fnv"
	"strconv"
	"strings"
	"unicode"
)

// BuildKey produces a canonical cache key for (protocol, method, height,
// paramsHash). The format is human-readable so the admin endpoint can
// dump entries without parsing back. Keep it short — keys live in a
// hashmap so byte-count multiplied by entry-count matters.
func BuildKey(protocol, method string, height int64, paramsHash uint64) string {
	var sb strings.Builder
	sb.Grow(len(protocol) + len(method) + 32)
	sb.WriteString(protocol)
	sb.WriteByte(':')
	sb.WriteString(method)
	sb.WriteByte(':')
	sb.WriteString(strconv.FormatInt(height, 10))
	sb.WriteByte(':')
	sb.WriteString(strconv.FormatUint(paramsHash, 16))
	return sb.String()
}

// HashParams produces a stable 64-bit hash of a JSON-RPC params payload.
// We assume the caller has already pulled out the height (separate key
// component); the hash covers the rest of params so two requests with
// the same height but different addresses don't collide.
//
// Unambiguous JSON is canonicalized: insignificant whitespace and object-key
// order do not affect the hash, but positional argument order does. Objects
// with repeated or case-fold-equivalent member names at any depth retain
// their original bytes because upstream decoders may interpret their order
// differently. JSON numbers retain their exact representation, including
// integers larger than 2^53. Non-JSON input (such as an encoded URI query) is
// hashed as-is; callers must keep those transports in separate namespaces.
func HashParams(b []byte) uint64 {
	if json.Valid(b) && !hasAmbiguousObjectNames(b) {
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		var params any
		if err := dec.Decode(&params); err == nil {
			if canonical, err := json.Marshal(params); err == nil {
				b = canonical
			}
		}
	}
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

// hasAmbiguousObjectNames scans already-valid JSON without decoding values.
// Only object scopes need tracking: a quoted string followed by a colon is
// necessarily a member name, even when objects appear inside nested arrays.
func hasAmbiguousObjectNames(b []byte) bool {
	var objects []map[string]struct{}
	for pos := 0; pos < len(b); pos++ {
		switch b[pos] {
		case '{':
			objects = append(objects, nil)
		case '}':
			objects[len(objects)-1] = nil
			objects = objects[:len(objects)-1]
		case '"':
			end := jsonStringEnd(b, pos)
			next := skipJSONSpace(b, end)
			if next < len(b) && b[next] == ':' {
				var name string
				if err := json.Unmarshal(b[pos:end], &name); err != nil {
					return true
				}
				name = foldMemberName(name)
				names := objects[len(objects)-1]
				if _, exists := names[name]; exists {
					return true
				}
				if names == nil {
					names = make(map[string]struct{})
					objects[len(objects)-1] = names
				}
				names[name] = struct{}{}
			}
			pos = end - 1
		}
	}
	return false
}

// Choose the smallest rune in each SimpleFold cycle so the comparison
// matches Unicode case folding, including aliases such as Kelvin sign/K.
func foldMemberName(name string) string {
	return strings.Map(func(r rune) rune {
		for {
			next := unicode.SimpleFold(r)
			if next <= r {
				return next
			}
			r = next
		}
	}, name)
}

// HashParamsExcept hashes b but skips bytes that match the height value
// the caller has already extracted. Useful when cache-keying by height
// in a separate component but preserving everything else in the params.
//
// Implementation is conservative: just hash the full bytes. If a future
// optimization needs to elide the height, replace this; the call sites
// are gated through this single helper.
func HashParamsExcept(b []byte, _ int64) uint64 {
	return HashParams(b)
}

// helpers used in tests / sub-packages

// PutUint64 writes a uint64 into a byte buffer for stable hashing of
// a numeric salt; exported for tests of the key shape.
func PutUint64(dst []byte, v uint64) {
	binary.LittleEndian.PutUint64(dst, v)
}
