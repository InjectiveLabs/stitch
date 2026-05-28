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
