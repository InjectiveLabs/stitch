package cosmos_grpc

import (
	"encoding/json"
	"io"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// Repeating readers let the wire-budget tests exercise a large body without
// allocating that body in either the fixture or the production projection.
type repeatedJSONByte struct {
	remaining int64
	value     byte
}

func (r *repeatedJSONByte) Read(dst []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := int(min(int64(len(dst)), r.remaining))
	for i := range dst[:n] {
		dst[i] = r.value
	}
	r.remaining -= int64(n)
	return n, nil
}

func largeIgnoredCometString(n int64) io.Reader {
	return io.MultiReader(
		strings.NewReader(`{"result":{"block":{"header":{"height":"127000000"},"data":{"txs":["`),
		&repeatedJSONByte{remaining: n, value: 'A'},
		strings.NewReader(`"]}}}}`),
	)
}

func TestCometProjectionMatchesJSONFieldSelection(t *testing.T) {
	tests := []struct{ input, want string }{
		{
			`{"jsonrpc":"2.0","id":1,"result":{"block_id":{"hash":"discard"},"block":{"header":{"height":"127000000","chain_id":"discard"},"data":{"txs":["abc\\def\u0061",null,true,42]},"last_commit":{}}}}`,
			`{"result":{"block":{"header":{"height":"127000000"}}}}`,
		},
		{
			`{"result":{"height":"127000000","txs_results":[{"code":0,"events":[{"type":"ignored"}]}],"finalize_block_events":[]}}`,
			`{"result":{"height":"127000000"}}`,
		},
		{
			`{"result":{"response":{"code":0,"log":"","height":"127000000","value":"ignored","proofOps":{"ops":[{"type":"ics23:iavl","key":"abc","data":"xyz"},{"type":"ics23:simple","data":"abc"}]} }}}`,
			`{"result":{"response":{"code":0,"log":"","height":"127000000","proofOps":{"ops":[{"type":"ics23:iavl","data":"present"}]}}}}`,
		},
		{
			`{"result":{"response":{"code":0,"height":"127000000","proof_ops":{"ops":[]}}}}`,
			`{"result":{"response":{"code":0,"height":"127000000","proof_ops":{"ops":[]}}}}`,
		},
		{
			`{"error":{"code":-32603,"message":"Internal error","data":{"reason":"height unavailable","attempts":[1,2.5,true,null]},"ignored":0}}`,
			`{"error":{"code":-32603,"message":"Internal error","data":{"reason":"height unavailable","attempts":[1,2.5,true,null]}}}`,
		},
		{
			`{"result":{"height":"first","height":"second","response":null},"error":null}`,
			`{"result":{"height":"second","response":null},"error":null}`,
		},
	}
	for _, tc := range tests {
		out, err := projectCometResponse(strings.NewReader(tc.input))
		if err != nil {
			t.Fatalf("project valid response: %v; input=%s", err, tc.input)
		}
		var actual, expected any
		if err := json.Unmarshal(out, &actual); err != nil {
			t.Fatalf("projected invalid JSON: %v; output=%s", err, out)
		}
		if err := json.Unmarshal([]byte(tc.want), &expected); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("projection mismatch: got=%s want=%s", out, tc.want)
		}
	}
}

func TestCometProjectionValidatesDiscardedJSON(t *testing.T) {
	invalid := []string{
		`{"ignored":[1,]}`, `{"ignored":{"a":1,}}`, `{"ignored":[,1]}`,
		`{"ignored":"bad\q"}`, `{"ignored":"bad\u12x4"}`, `{"ignored":"bad\u123"}`,
		"{\"ignored\":\"bad\ncontrol\"}", `{"ignored":"truncated`,
		`{"ignored":01}`, `{"ignored":1e}`, `{"ignored":--1}`, `{"ignored":fals}`,
		`{"result":{}} {}`, `{"result":{}}x`, `{"result":`,
		`{"ignored" 1}`, `{"ignored":[1 2]}`, `{"ignored":{"a":1 "b":2}}`,
	}
	for _, input := range invalid {
		if json.Valid([]byte(input)) {
			t.Fatalf("fixture should be invalid JSON: %s", input)
		}
		if out, err := projectCometResponse(strings.NewReader(input)); err == nil {
			t.Errorf("accepted malformed discarded data: input=%s output=%s", input, out)
		}
	}
	for _, value := range []string{`null`, `true`, `-2.4e+10`, `"escaped\\\"\n\u0000"`, `[{},[],1,"foo"]`} {
		input := `{"result":{"height":"7"},"ignored":` + value + `}`
		if out, err := projectCometResponse(strings.NewReader(input)); err != nil || string(out) != `{"result":{"height":"7"}}` {
			t.Errorf("valid discarded value failed: value=%s output=%s err=%v", value, out, err)
		}
	}
}

func TestCometProjectionBoundsMemoryForLargeDiscardedString(t *testing.T) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	out, err := projectCometResponse(largeIgnoredCometString(9 * 1024 * 1024))
	runtime.ReadMemStats(&after)
	if err != nil || string(out) != `{"result":{"block":{"header":{"height":"127000000"}}}}` {
		t.Fatalf("large discarded string projection: output=%s err=%v", out, err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 2*1024*1024 {
		t.Errorf("discarded 9MiB string allocated %d bytes; projection should not buffer that string", allocated)
	}
}

func TestCometProjectionEnforcesWireDepthAndMetadataBudgets(t *testing.T) {
	if _, err := projectCometResponse(largeIgnoredCometString(128 * 1024 * 1024)); err == nil || !strings.Contains(err.Error(), "128 MiB") {
		t.Errorf("wire limit not enforced: %v", err)
	}
	input := `{"ignored":` + strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65) + `}`
	if _, err := projectCometResponse(strings.NewReader(input)); err == nil || !strings.Contains(err.Error(), "64 levels") {
		t.Errorf("depth limit not enforced: %v", err)
	}
	input = `{"error":{"message":"` + strings.Repeat("A", 64*1024) + `"}}`
	if _, err := projectCometResponse(strings.NewReader(input)); err == nil || !strings.Contains(err.Error(), "64 KiB") {
		t.Errorf("selected scalar budget not enforced: %v", err)
	}
	input = `{"error":{"message":"` + strings.Repeat("A", 40*1024) + `","data":"` + strings.Repeat("B", 40*1024) + `"}}`
	if _, err := projectCometResponse(strings.NewReader(input)); err == nil || !strings.Contains(err.Error(), "64 KiB") {
		t.Errorf("aggregate projection budget not enforced: %v", err)
	}
}

func FuzzCometProjectionValidatesEntireJSON(f *testing.F) {
	for _, input := range []string{
		`{}`, `{"result":{"height":"127000000"}}`, `{"ignored":[1,]}`,
		`{"ignored":"escaped\\\"\u1234"}`, `{"result":null,"error":{"code":-32603,"data":"missing"}}`,
		`{"result":{"response":{"proofOps":{"ops":[null]}}}}`,
	} {
		f.Add([]byte(input))
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 16*1024 {
			t.Skip()
		}
		out, err := projectCometResponse(strings.NewReader(string(input)))
		if err != nil {
			return // Valid JSON can still exceed the depth/scalar budgets.
		}
		if !json.Valid(input) {
			t.Fatalf("accepted invalid JSON: %q -> %q", input, out)
		}
		if !json.Valid(out) {
			t.Fatalf("produced invalid JSON: %q -> %q", input, out)
		}
		if len(out) > 64*1024 {
			t.Fatalf("projection exceeded its output budget: %d", len(out))
		}
	})
}
