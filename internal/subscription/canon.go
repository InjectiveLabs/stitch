package subscription

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

// CanonicalJSON returns a stable byte representation of the input JSON.
// Equivalent inputs that differ only in object-key order or string-array
// order produce the same output, so two filters can hash to the same
// multicast key without hash collisions over insignificant differences.
//
// Sorted keys: object fields are emitted in lex order.
// Sorted arrays: a heuristic — if every element is a string, the array
// is sorted; otherwise order is preserved (sorting heterogenous arrays
// would change semantics).
//
// Numbers are normalized via Go's float roundtrip, so 1, 1.0, 1.00 all
// emit as "1". Nulls and booleans pass through unchanged.
func CanonicalJSON(raw json.RawMessage) ([]byte, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("canonicalize: %w", err)
	}
	canonical := canonicalize(v)
	out, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("canonicalize marshal: %w", err)
	}
	return out, nil
}

// CanonicalKey hashes the canonical JSON form into a 16-char hex string.
// Used as the lookup key in the multicast hub.
func CanonicalKey(raw json.RawMessage) (string, error) {
	canon, err := CanonicalJSON(raw)
	if err != nil {
		return "", err
	}
	h := fnv.New64a()
	_, _ = h.Write(canon)
	return fmt.Sprintf("%016x", h.Sum64()), nil
}

func canonicalize(v any) any {
	switch t := v.(type) {
	case map[string]any:
		// Sort keys.
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		// json.Marshal sorts map keys for us, but we still recurse so
		// nested arrays/objects get canonicalized.
		out := make(map[string]any, len(t))
		for _, k := range keys {
			out[k] = canonicalize(t[k])
		}
		return out
	case []any:
		recursed := make([]any, len(t))
		for i, e := range t {
			recursed[i] = canonicalize(e)
		}
		// Sort iff every element is a string. Mixed-type arrays preserve
		// order (sorting them would change downstream semantics).
		if allStrings(recursed) {
			cp := make([]string, len(recursed))
			for i, e := range recursed {
				cp[i] = e.(string)
			}
			sort.Strings(cp)
			out := make([]any, len(cp))
			for i, s := range cp {
				out[i] = s
			}
			return out
		}
		return recursed
	default:
		return v
	}
}

func allStrings(arr []any) bool {
	if len(arr) == 0 {
		return false
	}
	for _, e := range arr {
		if _, ok := e.(string); !ok {
			return false
		}
	}
	return true
}

// canonString is a debug helper that returns the canonical JSON as a
// Go string. Useful in tests when assertions need readable output.
func canonString(raw json.RawMessage) string {
	out, err := CanonicalJSON(raw)
	if err != nil {
		return strings.TrimSpace(string(raw))
	}
	return string(out)
}
