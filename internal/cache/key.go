package cache

import (
	"encoding/binary"
	"hash/fnv"
	"strconv"
	"strings"
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
// The hash is order-sensitive on JSON byte content. For methods where
// param order matters semantically (like positional args in EVM JSON-RPC)
// this is correct. For object-form params it's good-enough; clients
// don't shuffle keys between requests in practice.
func HashParams(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
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
