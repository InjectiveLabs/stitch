package health

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// cometStatusMock emulates a CometBFT /status endpoint returning a fixed
// latest_block_height.
func cometStatusMock(t *testing.T, height int64) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = fmt.Fprintf(w, `{"result":{"sync_info":{"latest_block_height":"%d","catching_up":false}}}`, height)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestBoundedVerifier_SuccessViaCometStatus(t *testing.T) {
	const upper = int64(141_500_000)
	srv := cometStatusMock(t, upper+1000) // backend has the upper block (and more)

	reg := backend.NewRegistry([]*backend.Backend{{
		Name:      "shard",
		Coverage:  backend.Coverage{Kind: backend.CovBounded, Lower: 119_000_000, Upper: upper},
		Endpoints: map[types.Protocol]string{types.ProtoRPC: srv.URL},
	}})
	h := NewRegistry()
	v := NewBoundedVerifier(reg, h)
	v.baseBackoff = 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v.Run(ctx)

	snap, ok := h.Get("shard", types.ProtoRPC)
	if !ok {
		t.Fatal("no snapshot published")
	}
	if !snap.Healthy {
		t.Errorf("expected healthy, got snap=%+v", snap)
	}
	if snap.LatestHeight != upper {
		t.Errorf("expected LatestHeight=%d (Upper), got %d", upper, snap.LatestHeight)
	}
}

func TestBoundedVerifier_FailureWhenBackendShortOfUpper(t *testing.T) {
	const upper = int64(141_500_000)
	srv := cometStatusMock(t, upper-1) // backend missing the declared upper

	reg := backend.NewRegistry([]*backend.Backend{{
		Name:      "shard",
		Coverage:  backend.Coverage{Kind: backend.CovBounded, Lower: 119_000_000, Upper: upper},
		Endpoints: map[types.Protocol]string{types.ProtoRPC: srv.URL},
	}})
	h := NewRegistry()
	v := NewBoundedVerifier(reg, h)
	v.maxAttempts = 2 // fail faster
	v.baseBackoff = 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v.Run(ctx)

	snap, ok := h.Get("shard", types.ProtoRPC)
	if !ok {
		t.Fatal("no snapshot published")
	}
	if snap.Healthy {
		t.Errorf("expected unhealthy, got snap=%+v", snap)
	}
	if snap.LastError == "" {
		t.Errorf("expected LastError to be set, got snap=%+v", snap)
	}
}

func TestBoundedVerifier_SkipsNonBounded(t *testing.T) {
	// /status would advance subAcked if hit; we expect no hits.
	var hits atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = fmt.Fprint(w, `{"result":{"sync_info":{"latest_block_height":"100","catching_up":false}}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	reg := backend.NewRegistry([]*backend.Backend{
		{
			Name:      "archive",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Endpoints: map[types.Protocol]string{types.ProtoRPC: srv.URL},
		},
		{
			Name:      "open",
			Coverage:  backend.Coverage{Kind: backend.CovOpen, Lower: 1},
			Endpoints: map[types.Protocol]string{types.ProtoRPC: srv.URL},
		},
		{
			Name:      "pruned",
			Coverage:  backend.Coverage{Kind: backend.CovPruned, Keep: 1000},
			Endpoints: map[types.Protocol]string{types.ProtoRPC: srv.URL},
		},
	})
	h := NewRegistry()
	v := NewBoundedVerifier(reg, h)
	v.baseBackoff = 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	v.Run(ctx)

	if got := hits.Load(); got != 0 {
		t.Errorf("verifier hit /status %d times; expected 0 (no bounded backends in registry)", got)
	}
}

func TestBoundedVerifier_SuccessViaEthRpcFallback(t *testing.T) {
	const upper = int64(141_500_000)

	// EVM JSON-RPC mock: eth_getBlockByNumber(0x...) returns a non-null result
	// for any block. Test that the verifier accepts that as proof of presence.
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"number":"0x86cd140","hash":"0xabc"}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	reg := backend.NewRegistry([]*backend.Backend{{
		Name:      "shard-evm-only",
		Coverage:  backend.Coverage{Kind: backend.CovBounded, Lower: 119_000_000, Upper: upper},
		Endpoints: map[types.Protocol]string{types.ProtoEthRPC: srv.URL},
	}})
	h := NewRegistry()
	v := NewBoundedVerifier(reg, h)
	v.baseBackoff = 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v.Run(ctx)

	snap, ok := h.Get("shard-evm-only", types.ProtoRPC)
	if !ok {
		t.Fatal("no snapshot published")
	}
	if !snap.Healthy {
		t.Errorf("expected healthy, got snap=%+v", snap)
	}
}

func TestBoundedVerifier_FailureWhenEthRpcReturnsNull(t *testing.T) {
	const upper = int64(141_500_000)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":null}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	reg := backend.NewRegistry([]*backend.Backend{{
		Name:      "shard-evm-missing",
		Coverage:  backend.Coverage{Kind: backend.CovBounded, Lower: 119_000_000, Upper: upper},
		Endpoints: map[types.Protocol]string{types.ProtoEthRPC: srv.URL},
	}})
	h := NewRegistry()
	v := NewBoundedVerifier(reg, h)
	v.maxAttempts = 2
	v.baseBackoff = 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v.Run(ctx)

	snap, ok := h.Get("shard-evm-missing", types.ProtoRPC)
	if !ok {
		t.Fatal("no snapshot published")
	}
	if snap.Healthy {
		t.Errorf("expected unhealthy, got snap=%+v", snap)
	}
}
