package forwarder

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/pool"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/types"
)

type stubSelector struct{ cands []*backend.Backend }

func (s stubSelector) Candidates(types.RouteKey) []*backend.Backend { return s.cands }

func mkBackend(name, base string) *backend.Backend {
	return &backend.Backend{
		Name:      name,
		Coverage:  backend.Coverage{Kind: backend.CovArchive},
		Weight:    100,
		Endpoints: map[types.Protocol]string{types.ProtoRPC: base},
	}
}

func newForwarder(s selector.Selector) *HTTP {
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   100 * time.Millisecond,
	})
	return NewHTTP(s, pool.NewHTTPPool(), cm, Policy{
		MaxAttempts:       3,
		PerAttemptTimeout: 2 * time.Second,
	})
}

func TestForwarderHappyPath(t *testing.T) {
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	fwd := newForwarder(stubSelector{cands: []*backend.Backend{mkBackend("a", upstream.URL)}})
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/status", nil)
	fwd.Forward(rec, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "status", Class: types.ClassLatest, Idempotent: true})

	if rec.Code != 200 {
		t.Fatalf("status %d body=%s", rec.Code, rec.Body.String())
	}
	if hits.Load() != 1 {
		t.Errorf("upstream hits: %d", hits.Load())
	}
}

func TestForwarderFailsOverOnConnectionRefused(t *testing.T) {
	var ok atomic.Int32
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		ok.Add(1)
		w.WriteHeader(200)
	}))
	defer good.Close()

	// Pick an obviously-unused port for the bad backend. Connection refused.
	bad := mkBackend("bad", "http://127.0.0.1:1") // port 1 is privileged + unused
	fwd := newForwarder(stubSelector{cands: []*backend.Backend{bad, mkBackend("good", good.URL)}})

	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/status", nil)
	fwd.Forward(rec, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "status", Class: types.ClassLatest, Idempotent: true})

	if rec.Code != 200 {
		t.Fatalf("expected 200 after failover, got %d", rec.Code)
	}
	if ok.Load() != 1 {
		t.Errorf("good upstream hits: %d", ok.Load())
	}
}

func TestForwarderFailsOverOn5xx(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(503)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer good.Close()

	fwd := newForwarder(stubSelector{cands: []*backend.Backend{
		mkBackend("bad", bad.URL),
		mkBackend("good", good.URL),
	}})
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/status", nil)
	fwd.Forward(rec, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "status", Class: types.ClassLatest, Idempotent: true})

	if rec.Code != 200 {
		t.Fatalf("expected 200 after retry, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestForwarderDoesNotRetryNonIdempotent(t *testing.T) {
	var attempts atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(503)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(200)
	}))
	defer good.Close()

	fwd := newForwarder(stubSelector{cands: []*backend.Backend{
		mkBackend("bad", bad.URL),
		mkBackend("good", good.URL),
	}})
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{"jsonrpc":"2.0","method":"broadcast_tx_sync"}`)))
	fwd.Forward(rec, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "broadcast_tx_sync", Class: types.ClassBroadcast, Idempotent: false})

	if rec.Code != 502 && rec.Code != 503 {
		t.Fatalf("expected error status without retry, got %d", rec.Code)
	}
	if attempts.Load() != 1 {
		t.Errorf("expected exactly 1 attempt, got %d", attempts.Load())
	}
}

func TestForwarderReplaysBodyOnRetry(t *testing.T) {
	var seen [2]string
	idx := atomic.Int32{}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		seen[idx.Add(1)-1] = string(body)
		w.WriteHeader(503)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		seen[idx.Add(1)-1] = string(body)
		w.WriteHeader(200)
	}))
	defer good.Close()

	fwd := newForwarder(stubSelector{cands: []*backend.Backend{
		mkBackend("bad", bad.URL),
		mkBackend("good", good.URL),
	}})
	body := `{"jsonrpc":"2.0","method":"block","params":{"height":"5"}}`
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	r.Header.Set("content-type", "application/json")
	fwd.Forward(rec, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "block", Class: types.ClassByHeight, Idempotent: true})

	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if seen[0] != body || seen[1] != body {
		t.Errorf("body not replayed: %q vs %q", seen[0], seen[1])
	}
}
