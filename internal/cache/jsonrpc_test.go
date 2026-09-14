package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestResponseWithIDPreservesPayloadBytes(t *testing.T) {
	// The escaped top-level key, nested ID, large integer, and whitespace
	// must survive unchanged; only the top-level ID token is substituted.
	const prefix = " { \"jsonrpc\" : \"2.0\", \"result\" : {\"id\":\"nested\", \"n\":900719925474099312345}, \"\\u0069d\" : "
	const suffix = ", \"extension\" : [ true, null ] }\n"
	body := []byte(prefix + `"original"` + suffix)
	original := bytes.Clone(body)
	for _, id := range []string{`900719925474099312345`, `"quoted-\"id\""`, `null`, `-1`, `"original"`} {
		got, ok := ResponseWithID(body, json.RawMessage(id))
		if !ok || string(got) != prefix+id+suffix {
			t.Errorf("ID %s: got %s, %v", id, got, ok)
		}
		if !bytes.Equal(body, original) {
			t.Error("rewriting a response mutated its input")
		}
	}
}

func TestResponseWithIDRejectsInvalidEnvelopes(t *testing.T) {
	for _, body := range []string{
		`not json`,
		`[]`,
		`null`,
		`{"jsonrpc":"2.0","result":"ok"}`,
		`{"jsonrpc":"2.0","id":1}`,
		`{"id":1,"result":"ok"}`,
		`{"jsonrpc":"1.0","id":1,"result":"ok"}`,
		`{"jsonrpc":"2.0","id":true,"result":"ok"}`,
		`{"jsonrpc":"2.0","id":{},"result":"ok"}`,
		`{"jsonrpc":"2.0","id":[],"result":"ok"}`,
		`{"jsonrpc":"2.0","id":1,"id":2,"result":"ok"}`,
		`{"jsonrpc":"2.0","id":1,"\u0069d":2,"result":"ok"}`,
		`{"jsonrpc":"2.0","id":1,"result":"first","result":"second"}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"retry later"}}`,
		`{"jsonrpc":"2.0","id":1,"result":null,"error":{"code":-32000}}`,
		`{"jsonrpc":"2.0","id":1,"result":"ok"} {}`,
	} {
		t.Run(body, func(t *testing.T) {
			if IsSuccessfulResponse([]byte(body)) {
				t.Error("invalid/error response accepted for storage")
			}
			if _, ok := ResponseWithID([]byte(body), json.RawMessage(`"client"`)); ok {
				t.Error("invalid/error response accepted for replay")
			}
		})
	}
	for _, id := range []string{"", "true", "false", "{}", "[]", "1 2", `"unterminated`} {
		if _, ok := ResponseWithID([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`), json.RawMessage(id)); ok {
			t.Errorf("invalid caller ID accepted: %q", id)
		}
	}
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":null,"result":null}`,
		`{"jsonrpc":"2.0","id":"client","result":null,"error":null}`,
	} {
		if !IsSuccessfulResponse([]byte(body)) {
			t.Errorf("valid nullable response rejected: %s", body)
		}
	}
}

func TestHashParamsCanonicalJSON(t *testing.T) {
	a := []byte(`[{"z":{"b":2,"a":9007199254740993},"x":[1,2]}]`)
	b := []byte(`[ { "x" : [ 1, 2 ], "z" : { "a" : 9007199254740993, "b" : 2 } } ]`)
	if HashParams(a) != HashParams(b) {
		t.Error("nested object order and whitespace changed parameter identity")
	}
	for _, changed := range []string{
		`[{"z":{"b":2,"a":9007199254740992},"x":[1,2]}]`,
		`[{"z":{"b":2,"a":9007199254740993},"x":[2,1]}]`,
		`[{"z":{"b":2,"a":"9007199254740993"},"x":[1,2]}]`,
	} {
		if HashParams(a) == HashParams([]byte(changed)) {
			t.Errorf("different numeric/string or positional params collided: %s", changed)
		}
	}
}

func BenchmarkResponseWithID(b *testing.B) {
	for _, size := range []int{512, 64 << 10, 128 << 10} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			body := []byte(`{"jsonrpc":"2.0","id":1,"result":"` + strings.Repeat("x", size) + `"}`)
			id := json.RawMessage(`900719925474099312345`)
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := ResponseWithID(body, id); !ok {
					b.Fatal("valid cached response rejected")
				}
			}
		})
	}
}
