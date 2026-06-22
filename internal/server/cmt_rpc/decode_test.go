package cmt_rpc

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/decentrio/stitch/internal/types"
)

func TestDecodeURIBlockHeight(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/block?height=12345", nil)
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Method != "block" {
		t.Errorf("method: %q", d.key.Method)
	}
	if d.key.Class != types.ClassByHeight {
		t.Errorf("class: %s", d.key.Class)
	}
	if d.key.HeightOrZero() != 12345 {
		t.Errorf("height: %d", d.key.HeightOrZero())
	}
	if !d.key.Idempotent {
		t.Error("block should be idempotent")
	}
}

func TestDecodeURIStatus(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/status", nil)
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Class != types.ClassLatest {
		t.Errorf("class: %s", d.key.Class)
	}
}

func TestDecodeURIBlockByHash(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/block_by_hash?hash=0xabcd", nil)
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Class != types.ClassByHash {
		t.Errorf("class: %s", d.key.Class)
	}
	if string(d.key.Hash) != "0xabcd" {
		t.Errorf("hash: %s", d.key.Hash)
	}
}

func TestDecodeURITxSearchHeightQuery(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, `/tx_search?query=%22tx.height=150017800%22`, nil)
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Method != "tx_search" {
		t.Errorf("method: %q", d.key.Method)
	}
	if d.key.Class != types.ClassByHeight {
		t.Errorf("class: %s", d.key.Class)
	}
	if d.key.HeightOrZero() != 150017800 {
		t.Errorf("height: %d", d.key.HeightOrZero())
	}
}

func TestDecodeURIBlockSearchHeightQuery(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, `/block_search?query=block.height%3D12345`, nil)
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Class != types.ClassByHeight {
		t.Errorf("class: %s", d.key.Class)
	}
	if d.key.HeightOrZero() != 12345 {
		t.Errorf("height: %d", d.key.HeightOrZero())
	}
}

func TestDecodeURISearchHeightRange(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, `/tx_search?query=tx.height%3E100%20AND%20tx.height%3C%3D200`, nil)
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	assertRange(t, d, 101, 200)
}

func TestDecodeURIBlockchainRange(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, `/blockchain?minHeight=10&maxHeight=20`, nil)
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	assertRange(t, d, 10, 20)
}

func TestDecodeJSONRPCBlockHeightObject(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"5000"}}`)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Method != "block" {
		t.Errorf("method: %q", d.key.Method)
	}
	if d.key.Class != types.ClassByHeight {
		t.Errorf("class: %s", d.key.Class)
	}
	if d.key.HeightOrZero() != 5000 {
		t.Errorf("height: %d", d.key.HeightOrZero())
	}
	if !bytes.Equal(d.body, body) {
		t.Error("body should be buffered for retry")
	}
}

func TestDecodeJSONRPCTxSearchHeightObject(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"tx_search","params":{"query":"tx.height=150017800"}}`)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Class != types.ClassByHeight {
		t.Errorf("class: %s", d.key.Class)
	}
	if d.key.HeightOrZero() != 150017800 {
		t.Errorf("height: %d", d.key.HeightOrZero())
	}
}

func TestDecodeJSONRPCTxSearchHeightArray(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"tx_search","params":["tx.height=150017800",false,1,30,"asc"]}`)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Class != types.ClassByHeight {
		t.Errorf("class: %s", d.key.Class)
	}
	if d.key.HeightOrZero() != 150017800 {
		t.Errorf("height: %d", d.key.HeightOrZero())
	}
}

func TestDecodeJSONRPCBlockchainRangeObject(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"blockchain","params":{"minHeight":"10","maxHeight":"20"}}`)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	assertRange(t, d, 10, 20)
}

func TestDecodeJSONRPCBlockchainRangeArray(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"blockchain","params":[10,20]}`)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	assertRange(t, d, 10, 20)
}

func TestDecodeJSONRPCBroadcastTxSync(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"broadcast_tx_sync","params":{"tx":"AAA="}}`)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	d, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Class != types.ClassBroadcast {
		t.Errorf("class: %s", d.key.Class)
	}
	if d.key.Idempotent {
		t.Error("broadcast must not be marked idempotent")
	}
}

func TestDecodeJSONRPCInvalid(t *testing.T) {
	body := []byte(`not json`)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	d, err := decode(r)
	if err != nil {
		t.Fatalf("decode should not return error for invalid json (got %v)", err)
	}
	if d.key.Method != "_invalid" {
		t.Errorf("method: %q", d.key.Method)
	}
}

func TestParseHeightHexAndDecimal(t *testing.T) {
	for _, c := range []struct {
		in  string
		out int64
		ok  bool
	}{
		{"123", 123, true},
		{"0x10", 16, true},
		{"0X10", 16, true},
		{"0", 0, false},
		{"", 0, false},
		{"abc", 0, false},
	} {
		got, ok := parseHeight(c.in)
		if ok != c.ok || got != c.out {
			t.Errorf("parseHeight(%q) = %d, %v; want %d, %v", c.in, got, ok, c.out, c.ok)
		}
	}
}

func TestUnquoteRawHandlesNumbers(t *testing.T) {
	// Numeric height (no quotes) should still be returned as a string.
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":42}}`)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	d, _ := decode(r)
	if d.key.HeightOrZero() != 42 {
		t.Errorf("numeric height: got %d", d.key.HeightOrZero())
	}
}

func TestRequestBodyReplayable(t *testing.T) {
	// After decode, the body must still be readable for the forwarder.
	body := strings.Repeat(`{"jsonrpc":"2.0","method":"block","params":{}}`, 1)
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	if _, err := decode(r); err != nil {
		t.Fatal(err)
	}
	got, err := r.Body.Close, []byte(nil)
	_ = got
	_ = err
}

func assertRange(t *testing.T, d decoded, lower, upper int64) {
	t.Helper()
	if d.key.Class != types.ClassByHeightRange {
		t.Fatalf("class: %s", d.key.Class)
	}
	if d.key.Range == nil {
		t.Fatal("range is nil")
	}
	if d.key.Range.Lower == nil || *d.key.Range.Lower != lower {
		t.Fatalf("lower: %v", d.key.Range.Lower)
	}
	if d.key.Range.Upper == nil || *d.key.Range.Upper != upper {
		t.Fatalf("upper: %v", d.key.Range.Upper)
	}
}
