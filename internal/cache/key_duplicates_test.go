package cache

import (
	"hash/fnv"
	"testing"
)

func TestHashParamsDuplicateNamesUseRawBytes(t *testing.T) {
	for _, tc := range []struct {
		name, duplicate, lastValueOnly string
	}{
		{"top level", `{"page":1,"page":2}`, `{"page":2}`},
		{"escaped equivalent", `{"page":1,"\u0070age":2}`, `{"page":2}`},
		{"same value", `{"page":2,"page":2}`, `{"page":2}`},
		{"nested object", `{"query":{"page":1,"page":2}}`, `{"query":{"page":2}}`},
		{"nested arrays", `[{},[{"page":1,"page":2}]]`, `[{},[{"page":2}]]`},
		{"nested escaped equivalent", `[{"query":[{"page":1,"\u0070age":2}]}]`, `[{"query":[{"page":2}]}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hash := fnv.New64a()
			_, _ = hash.Write([]byte(tc.duplicate))
			if got := HashParams([]byte(tc.duplicate)); got != hash.Sum64() {
				t.Errorf("duplicate-name params were canonicalized: hash=%x; want raw hash=%x", got, hash.Sum64())
			}
			if HashParams([]byte(tc.duplicate)) == HashParams([]byte(tc.lastValueOnly)) {
				t.Error("ambiguous duplicate names aliased ordinary last-value-only params")
			}
			// Conservative fallback preserves even harmless formatting, since
			// duplicate-name semantics belong to the upstream JSON parser.
			spaced := " \n" + tc.duplicate + "\t "
			hash.Reset()
			_, _ = hash.Write([]byte(spaced))
			if got := HashParams([]byte(spaced)); got != hash.Sum64() {
				t.Errorf("fallback discarded original whitespace: got=%x; want=%x", got, hash.Sum64())
			}
			if HashParams([]byte(spaced)) == HashParams([]byte(tc.duplicate)) {
				t.Error("ambiguous params with different raw bytes shared a hash")
			}
		})
	}
}

func TestHashParamsRepeatedNamesInDistinctObjectsCanonicalize(t *testing.T) {
	// Reusing a field name in sibling objects is unambiguous. String values
	// that resemble duplicate members must not trigger the raw fallback.
	a := []byte(`[{"page":1,"text":"\"page\":1,\"page\":2"},{"page":2,"query":{"page":3}}]`)
	b := []byte(` [ { "text" : "\"page\":1,\"page\":2", "\u0070age" : 1 }, { "query" : { "page" : 3 }, "page" : 2 } ] `)
	if HashParams(a) != HashParams(b) {
		t.Error("unique object members lost canonical identity across field order/whitespace/escapes")
	}
}

func TestHashParamsCaseAliasesKeepOriginalOrder(t *testing.T) {
	for _, tc := range []struct{ name, first, reversed string }{
		{"ASCII", `{"blockNumber":"0x1","BlockNumber":"0x2"}`, `{"BlockNumber":"0x2","blockNumber":"0x1"}`},
		{"escaped ASCII", `[{"blockNumber":"0x1","\u0042lockNumber":"0x2"}]`, `[{"\u0042lockNumber":"0x2","blockNumber":"0x1"}]`},
		{"Kelvin", `{"query":{"K":1,"K":2}}`, `{"query":{"K":2,"K":1}}`},
		{"escaped Kelvin", `{"query":{"K":1,"\u212a":2}}`, `{"query":{"\u212a":2,"K":1}}`},
		{"long s", `{"S":1,"ſ":2}`, `{"ſ":2,"S":1}`},
		{"sigma", `[{"query":[{"Σ":1,"ς":2}]}]`, `[{"query":[{"ς":2,"Σ":1}]}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if HashParams([]byte(tc.first)) == HashParams([]byte(tc.reversed)) {
				t.Error("case-insensitive aliases with different member order shared a hash")
			}
			for _, raw := range []string{tc.first, tc.reversed} {
				hash := fnv.New64a()
				_, _ = hash.Write([]byte(raw))
				if got := HashParams([]byte(raw)); got != hash.Sum64() {
					t.Errorf("case aliases were canonicalized: hash=%x; want raw hash=%x", got, hash.Sum64())
				}
			}
		})
	}
}
