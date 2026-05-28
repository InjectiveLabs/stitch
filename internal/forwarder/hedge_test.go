package forwarder

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/pool"
	"github.com/decentrio/stitch/internal/types"
)

func newHedgeForwarder(s stubSelector, after time.Duration) *HTTP {
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   100 * time.Millisecond,
	})
	return NewHTTP(s, pool.NewHTTPPool(), cm, Policy{
		MaxAttempts:       3,
		PerAttemptTimeout: 2 * time.Second,
		HedgeAfter:        after,
	})
}

func TestHedgeWinsWhenPrimarySlow(t *testing.T) {
	var primaryHits, fastHits atomic.Int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryHits.Add(1)
		time.Sleep(300 * time.Millisecond) // slow
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`from-slow`))
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fastHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`from-fast`))
	}))
	defer fast.Close()

	cands := []*backend.Backend{mkBackend("slow", slow.URL), mkBackend("fast", fast.URL)}
	fwd := newHedgeForwarder(stubSelector{cands: cands}, 50*time.Millisecond)

	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/eth_call", nil)
	fwd.Hedge(rec, r, types.RouteKey{
		Protocol:   types.ProtoRPC,
		Method:     "eth_call",
		Class:      types.ClassByHeight,
		Idempotent: true,
		Hedge:      true,
	})

	if rec.Code != 200 {
		t.Fatalf("status %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "from-fast" {
		t.Errorf("expected from-fast (hedge winner); got %q", rec.Body.String())
	}
	if fastHits.Load() != 1 {
		t.Errorf("hedge backend hits: %d", fastHits.Load())
	}
}

func TestHedgeFallsBackToForwardWhenSingleCandidate(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`only-one`))
	}))
	defer good.Close()

	fwd := newHedgeForwarder(stubSelector{cands: []*backend.Backend{mkBackend("only", good.URL)}}, 10*time.Millisecond)

	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/eth_call", nil)
	fwd.Hedge(rec, r, types.RouteKey{
		Protocol:   types.ProtoRPC,
		Method:     "eth_call",
		Class:      types.ClassByHeight,
		Idempotent: true,
		Hedge:      true,
	})
	if rec.Code != 200 || rec.Body.String() != "only-one" {
		t.Errorf("status %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestHedgeBothFailReturns502(t *testing.T) {
	bad1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(503)
	}))
	defer bad1.Close()
	bad2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(503)
	}))
	defer bad2.Close()

	fwd := newHedgeForwarder(stubSelector{cands: []*backend.Backend{
		mkBackend("a", bad1.URL),
		mkBackend("b", bad2.URL),
	}}, 10*time.Millisecond)

	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/eth_call", nil)
	fwd.Hedge(rec, r, types.RouteKey{
		Protocol:   types.ProtoRPC,
		Method:     "eth_call",
		Class:      types.ClassByHeight,
		Idempotent: true,
		Hedge:      true,
	})
	if rec.Code != http.StatusBadGateway {
		t.Errorf("expected 502; got %d", rec.Code)
	}
}
