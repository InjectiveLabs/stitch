package forwarder

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/types"
)

// Broadcast must go through Acquire, not the read-only Allow: a half-open
// backend whose single canary slot is already claimed gets no fan-out
// request.
func TestBroadcastSkipsHalfOpenWithClaimedCanary(t *testing.T) {
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
	}))
	defer upstream.Close()

	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   10 * time.Millisecond,
	})
	cm.Record("a", types.ProtoRPC, false)
	cm.Record("a", types.ProtoRPC, false) // tripped
	time.Sleep(15 * time.Millisecond)     // cooldown elapses
	if !cm.Acquire("a", types.ProtoRPC) {
		t.Fatal("test setup: canary slot should be claimable")
	}

	fwd := newForwarderWithCircuit(stubSelector{cands: []*backend.Backend{mkBackend("a", upstream.URL)}}, cm, 3)
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","method":"broadcast_tx_sync"}`))
	fwd.Broadcast(rec, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "broadcast_tx_sync", Class: types.ClassBroadcast, Idempotent: false})

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 while the canary slot is claimed, got %d", rec.Code)
	}
	if hits.Load() != 0 {
		t.Errorf("no request should be dispatched past a claimed canary slot; hits=%d", hits.Load())
	}
}

// While a half-open canary broadcast is in flight, a second Broadcast must
// be rejected: dispatching claims the slot, not just reads it.
func TestBroadcastCanaryClaimedWhileInFlight(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		<-release
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()
	defer close(release)

	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   10 * time.Millisecond,
	})
	cm.Record("a", types.ProtoRPC, false)
	cm.Record("a", types.ProtoRPC, false) // tripped
	time.Sleep(15 * time.Millisecond)     // cooldown elapses; slot still free

	fwd := newForwarderWithCircuit(stubSelector{cands: []*backend.Backend{mkBackend("a", upstream.URL)}}, cm, 3)
	key := types.RouteKey{Protocol: types.ProtoRPC, Method: "broadcast_tx_sync", Class: types.ClassBroadcast, Idempotent: false}

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"method":"broadcast_tx_sync"}`))
		fwd.Broadcast(rec, r, key)
		done <- rec
	}()

	// Wait until the canary is in flight (blocked in the upstream handler).
	deadline := time.After(2 * time.Second)
	for hits.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("canary broadcast never reached the upstream")
		case <-time.After(time.Millisecond):
		}
	}

	rec2 := httptest.NewRecorder()
	r2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"method":"broadcast_tx_sync"}`))
	fwd.Broadcast(rec2, r2, key)
	if rec2.Code != http.StatusServiceUnavailable {
		t.Errorf("second broadcast must be rejected while the canary is in flight; got %d", rec2.Code)
	}

	release <- struct{}{}
	rec1 := <-done
	if rec1.Code != 200 {
		t.Errorf("canary broadcast should succeed; got %d", rec1.Code)
	}
	if st := cm.State("a", types.ProtoRPC); st != circuit.StateClosed {
		t.Errorf("successful canary should close the breaker; state=%s", st)
	}
}
