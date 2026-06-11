package forwarder

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/metrics"
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
	return newForwarderWithCircuit(s, cm, 3)
}

func newForwarderWithCircuit(s selector.Selector, cm *circuit.Manager, maxAttempts int) *HTTP {
	return NewHTTP(s, pool.NewHTTPPool(), cm, Policy{
		MaxAttempts:       maxAttempts,
		PerAttemptTimeout: 2 * time.Second,
	})
}

func newTestCircuit() *circuit.Manager {
	return circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   time.Minute,
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

// A breaker that trips between candidate selection and the attempt must be
// skipped at attempt time, and the skip must not consume an attempt slot.
func TestForwardSkipsTrippedCandidateWithoutConsumingAttempt(t *testing.T) {
	var badHits, goodHits atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		badHits.Add(1)
		w.WriteHeader(503)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		goodHits.Add(1)
		w.WriteHeader(200)
	}))
	defer good.Close()

	cm := newTestCircuit()
	// Trip "bad" after selection would have happened: the stub selector
	// does no circuit filtering, so the candidate list still contains it.
	cm.Record("bad", types.ProtoRPC, false)
	cm.Record("bad", types.ProtoRPC, false)
	if cm.State("bad", types.ProtoRPC) != circuit.StateOpen {
		t.Fatal("test setup: breaker should be open")
	}

	// MaxAttempts=1: if the skip consumed the attempt, "good" would never
	// be reached.
	fwd := newForwarderWithCircuit(stubSelector{cands: []*backend.Backend{
		mkBackend("bad", bad.URL),
		mkBackend("good", good.URL),
	}}, cm, 1)
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/status", nil)
	fwd.Forward(rec, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "status", Class: types.ClassLatest, Idempotent: true})

	if rec.Code != 200 {
		t.Fatalf("expected 200 from the healthy candidate, got %d body=%s", rec.Code, rec.Body.String())
	}
	if badHits.Load() != 0 {
		t.Errorf("tripped candidate must not be attempted; hits=%d", badHits.Load())
	}
	if goodHits.Load() != 1 {
		t.Errorf("good upstream hits: %d", goodHits.Load())
	}
}

// An upstream that dies mid-body must be debited as a circuit failure and
// counted by stitch_relay_truncated_total, even though headers (and part of
// the body) already reached the client.
func TestForwardTruncatedUpstreamRecordsFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("short"))
	}))
	defer upstream.Close()

	cm := newTestCircuit()
	fwd := newForwarderWithCircuit(stubSelector{cands: []*backend.Backend{mkBackend("trunc", upstream.URL)}}, cm, 1)

	before := testutil.ToFloat64(metrics.RelayTruncated.WithLabelValues("trunc", string(types.ProtoRPC)))
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/status", nil)
		fwd.Forward(rec, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "status", Class: types.ClassLatest, Idempotent: true})
		if rec.Code != 200 {
			t.Fatalf("headers were already relayed; expected 200, got %d", rec.Code)
		}
	}

	if got := testutil.ToFloat64(metrics.RelayTruncated.WithLabelValues("trunc", string(types.ProtoRPC))) - before; got != 2 {
		t.Errorf("RelayTruncated delta: got %v, want 2", got)
	}
	if st := cm.State("trunc", types.ProtoRPC); st != circuit.StateOpen {
		t.Errorf("two truncated bodies must trip the breaker; state=%s", st)
	}
}

// failingWriter simulates a client that went away mid-response: every body
// write fails while headers still succeed.
type failingWriter struct{ http.ResponseWriter }

func (f *failingWriter) Write([]byte) (int, error) { return 0, errors.New("client write: broken pipe") }

// A client-side write failure must not be blamed on the backend: the
// upstream served the response fine, so the circuit records a success.
func TestForwardClientWriteFailureDoesNotBlameBackend(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	cm := newTestCircuit()
	fwd := newForwarderWithCircuit(stubSelector{cands: []*backend.Backend{mkBackend("cw", upstream.URL)}}, cm, 1)

	before := testutil.ToFloat64(metrics.RelayTruncated.WithLabelValues("cw", string(types.ProtoRPC)))
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/status", nil)
		fwd.Forward(&failingWriter{ResponseWriter: rec}, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "status", Class: types.ClassLatest, Idempotent: true})
	}

	if st := cm.State("cw", types.ProtoRPC); st != circuit.StateClosed {
		t.Errorf("client write failures must not trip the backend's breaker; state=%s", st)
	}
	if got := testutil.ToFloat64(metrics.RelayTruncated.WithLabelValues("cw", string(types.ProtoRPC))) - before; got != 0 {
		t.Errorf("client write failures must not count as truncated relays; delta=%v", got)
	}
}

// A non-retryable 5xx (plain 500) passes through to the client but must be
// recorded as a circuit failure, not a success.
func TestForward500PassThroughRecordsCircuitFailure(t *testing.T) {
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer upstream.Close()

	cm := newTestCircuit()
	fwd := newForwarderWithCircuit(stubSelector{cands: []*backend.Backend{mkBackend("ise", upstream.URL)}}, cm, 3)

	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/status", nil)
		fwd.Forward(rec, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "status", Class: types.ClassLatest, Idempotent: true})
		if rec.Code != 500 {
			t.Fatalf("500 must pass through unchanged, got %d", rec.Code)
		}
		if rec.Body.String() != `{"error":"boom"}` {
			t.Fatalf("500 body must pass through, got %q", rec.Body.String())
		}
	}

	if hits.Load() != 2 {
		t.Errorf("500 must not be retried; upstream hits=%d", hits.Load())
	}
	if st := cm.State("ise", types.ProtoRPC); st != circuit.StateOpen {
		t.Errorf("two 500s must trip the breaker; state=%s", st)
	}
}

// When the client has already disconnected, Forward must stop before trying
// any candidate: nothing written, nothing recorded.
func TestForwardClientGoneStopsAttempts(t *testing.T) {
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
	}))
	defer upstream.Close()

	fwd := newForwarder(stubSelector{cands: []*backend.Backend{mkBackend("u", upstream.URL)}})
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/status", nil)
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	r = r.WithContext(ctx)

	fwd.Forward(rec, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "status", Class: types.ClassLatest, Idempotent: true})

	if rec.Body.Len() != 0 {
		t.Errorf("nothing should be written for a gone client; got %q", rec.Body.String())
	}
	if hits.Load() != 0 {
		t.Errorf("no upstream attempt should be made; hits=%d", hits.Load())
	}
}
