package eth_rpc

import (
	"encoding/json"
	"testing"
)

func TestParamArrayAccess(t *testing.T) {
	raw := json.RawMessage(`["0xabc", "0x12", true]`)
	if string(param(raw, 0)) != `"0xabc"` {
		t.Errorf("param[0]: %s", param(raw, 0))
	}
	if string(param(raw, 1)) != `"0x12"` {
		t.Errorf("param[1]: %s", param(raw, 1))
	}
	if param(raw, 99) != nil {
		t.Error("out-of-range should be nil")
	}
	if param(json.RawMessage(`{"x":1}`), 0) != nil {
		t.Error("non-array should be nil")
	}
}

func TestParseHexUint64(t *testing.T) {
	for _, c := range []struct {
		in  string
		out int64
		ok  bool
	}{
		{"0x10", 16, true},
		{"0X10", 16, true},
		{"0x0", 0, true},
		{"earliest", 1, true},
		{"latest", 0, false},
		{"pending", 0, false},
		{"safe", 0, false},
		{"finalized", 0, false},
		{"123", 123, true},
		{"", 0, false},
		{"\"0x10\"", 16, true},
	} {
		got, ok := parseHexUint64(c.in)
		if got != c.out || ok != c.ok {
			t.Errorf("parseHexUint64(%q) = %d, %v; want %d, %v", c.in, got, ok, c.out, c.ok)
		}
	}
}

func TestExtractBlockNumber(t *testing.T) {
	for _, c := range []struct {
		raw  string
		out  int64
		ok   bool
	}{
		{`"0x10"`, 16, true},
		{`"latest"`, 0, false},
		{`{"blockNumber":"0x42"}`, 0x42, true},
		{`{"blockHash":"0xabcd"}`, 0, false},
		{`null`, 0, false},
	} {
		got, ok := extractBlockNumber(json.RawMessage(c.raw))
		if got != c.out || ok != c.ok {
			t.Errorf("extractBlockNumber(%s) = %d, %v; want %d, %v", c.raw, got, ok, c.out, c.ok)
		}
	}
}

func TestExtractBlockHash(t *testing.T) {
	hash64 := "0x" + repeat("ab", 32)
	for _, c := range []struct {
		raw string
		ok  bool
	}{
		{`"` + hash64 + `"`, true},
		{`{"blockHash":"` + hash64 + `"}`, true},
		{`{"blockNumber":"0x10"}`, false},
		{`"latest"`, false},
		{`null`, false},
	} {
		_, ok := extractBlockHash(json.RawMessage(c.raw))
		if ok != c.ok {
			t.Errorf("extractBlockHash(%s) ok=%v; want %v", c.raw, ok, c.ok)
		}
	}
}

func TestExtractFilterID(t *testing.T) {
	for _, c := range []struct {
		raw string
		out string
		ok  bool
	}{
		{`"0xabc123"`, "0xabc123", true},
		{`"abc"`, "", false},
		{`"0x"`, "", false},
		{`null`, "", false},
		{`123`, "", false},
	} {
		got, ok := extractFilterID(json.RawMessage(c.raw))
		if got != c.out || ok != c.ok {
			t.Errorf("extractFilterID(%s) = %q, %v; want %q, %v", c.raw, got, ok, c.out, c.ok)
		}
	}
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}
