package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestResponseWithIDEnvelopeLayouts(t *testing.T) {
	results := []string{
		`null`, `true`, `false`, `-900719925474099312345`, `-1.23456789e+100`,
		`[]`, `{}`, `[{},[],[null,{"id":12}]]`,
		`"delimiters } ] , : { [ and \"quotes\""`,
		`"ends in escaped backslash\\"`,
		`"backslash then quote\\\" and comma,"`,
		`{"id":"nested","text":"} , [ \"quoted\" \\ path","list":[true,false,null,{"deep":[1,{"s":"\\\"}"}]}]}`,
		strings.Repeat("[", 4096) + "null" + strings.Repeat("]", 4096),
	}
	layouts := []string{
		`{"id":%[1]s,"result":%[2]s,"jsonrpc":"2.0"}`,
		`{"jsonrpc":"2.0","id":%[1]s,"result":%[2]s}`,
		` { "result" : %[2]s, "jsonrpc" : "2.0", "id" : %[1]s } `,
		"\n{\"extension\":{\"id\":{},\"text\":\"} ],\\\"\"},\"\\u0072esult\":%[2]s,\"\\u0069d\" : %[1]s,\"\\u006asonrpc\":\"2.0\"}\t\r\n",
	}
	for resultIndex, result := range results {
		for layoutIndex, layout := range layouts {
			t.Run(fmt.Sprintf("result%d/layout%d", resultIndex, layoutIndex), func(t *testing.T) {
				body := []byte(fmt.Sprintf(layout, `"old-\\\"id"`, result))
				if !json.Valid(body) {
					t.Fatalf("invalid test fixture: %s", body)
				}
				original := bytes.Clone(body)
				for _, id := range []string{`null`, `-1.5e20`, `900719925474099312345`, `"client-\\\"id"`} {
					got, ok := ResponseWithID(body, json.RawMessage(id))
					want := fmt.Sprintf(layout, id, result)
					if !ok || string(got) != want {
						t.Errorf("ID %s: response changed outside ID or was rejected: ok=%v", id, ok)
					}
					if !bytes.Equal(body, original) {
						t.Error("input response mutated")
					}
				}
			})
		}
	}
}

func TestResponseWithIDEscapedDuplicateFields(t *testing.T) {
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":1,"\u0069d":2,"result":null}`,
		`{"jsonrpc":"2.0","id":1,"result":null,"\u0072esult":true}`,
		`{"jsonrpc":"2.0","id":1,"result":null,"\u006asonrpc":"2.0"}`,
		`{"jsonrpc":"2.0","id":1,"result":null,"error":null,"\u0065rror":null}`,
	} {
		if IsSuccessfulResponse([]byte(body)) {
			t.Errorf("escaped duplicate field accepted: %s", body)
		}
		if _, ok := ResponseWithID([]byte(body), json.RawMessage(`"caller"`)); ok {
			t.Errorf("escaped duplicate field replayed: %s", body)
		}
	}
}

func FuzzResponseWithIDPreservesEnvelope(f *testing.F) {
	for _, result := range []string{
		`null`, `true`, `-1.23e-100`, `900719925474099312345`, `[]`, `{}`,
		`"\\"`, `"\""`, `"\\\""`, `"}],:\""`,
		`{"id":1,"a":[{},[null,true,false,123],"} ], \""]}`,
	} {
		f.Add([]byte(result), "caller-\\\"id")
	}
	f.Fuzz(func(t *testing.T, result []byte, caller string) {
		if len(result) > 1<<20 || len(caller) > 1<<20 || !json.Valid(result) {
			return
		}
		id, err := json.Marshal(caller)
		if err != nil {
			t.Fatal(err)
		}
		const prefix = " {\"jsonrpc\":\"2.0\",\"result\": "
		const middle = ",\"\\u0069d\" : "
		const suffix = ",\"extension\":[{},true,null]} \n"
		body := []byte(prefix + string(result) + middle + `"original"` + suffix)
		// Wrapping a result at encoding/json's maximum nesting depth adds
		// another level, so only require rewriting valid complete envelopes.
		if !json.Valid(body) {
			return
		}
		original := bytes.Clone(body)
		got, ok := ResponseWithID(body, id)
		want := prefix + string(result) + middle + string(id) + suffix
		if !ok || string(got) != want {
			t.Fatalf("valid response rejected or bytes outside caller ID changed: ok=%v", ok)
		}
		if !bytes.Equal(body, original) {
			t.Fatal("input response mutated")
		}
	})
}
