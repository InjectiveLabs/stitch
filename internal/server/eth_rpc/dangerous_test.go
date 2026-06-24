package eth_rpc

import (
	"io"
	"strings"
	"testing"
)

func TestDangerousAllowlistDeniesByDefault(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	// No allowlist set → debug_traceTransaction is denied.
	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"debug_traceTransaction","params":["0xabc"]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "method not found") {
		t.Fatalf("expected method-not-found; got %s", body)
	}
}

func TestDangerousAllowlistGrantsExplicitMethod(t *testing.T) {
	r := setupEth(t)
	defer r.close()
	r.front.SetDangerousAllowlist(NewDangerousAllowlist([]string{"debug_traceTransaction"}))

	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"debug_traceTransaction","params":["0xabc"]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "method not found") {
		t.Fatalf("debug_traceTransaction was allowed; should reach upstream: %s", body)
	}
}

func TestDangerousAllowlistWildcardGrantsHiddenMethods(t *testing.T) {
	r := setupEth(t)
	defer r.close()
	r.front.SetDangerousAllowlist(NewDangerousAllowlist([]string{"*"}))

	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"debug_traceCall","params":[{},"0x1"]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "method not found") {
		t.Fatalf("debug_traceCall was allowed by wildcard; should reach upstream: %s", body)
	}
	if r.archive.hits.Load()+r.shard.hits.Load() == 0 {
		t.Fatal("wildcard-allowed method did not reach upstream")
	}
}

func TestDangerousAllowlistDoesNotLeakOtherMethods(t *testing.T) {
	r := setupEth(t)
	defer r.close()
	// Allow debug_traceTransaction only.
	r.front.SetDangerousAllowlist(NewDangerousAllowlist([]string{"debug_traceTransaction"}))

	// debug_traceCall should still be denied.
	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"debug_traceCall","params":[{},"0x1"]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "method not found") {
		t.Fatalf("debug_traceCall should still be denied: %s", body)
	}
}

func TestDangerousAllowlistEmptySliceIsSafeNoOp(t *testing.T) {
	d := NewDangerousAllowlist(nil)
	if d.Allowed("debug_traceCall") {
		t.Error("nil/empty allowlist should deny everything")
	}
	if d.Allowed("") {
		t.Error("empty method should not be allowed")
	}
}

func TestDangerousAllowlistWildcardStillRejectsEmptyMethod(t *testing.T) {
	d := NewDangerousAllowlist([]string{"*"})
	if d.Allowed("") {
		t.Error("wildcard should not allow an empty method")
	}
}
