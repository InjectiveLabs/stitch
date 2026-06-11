package forwarder

import (
	"context"
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

// A secondary whose breaker rejects admission at timer-fire must not be
// dispatched: the primary continues alone and the secondary's breaker is
// left untouched.
func TestHedgeSkipsUnacquirableSecondary(t *testing.T) {
	var secondaryHits atomic.Int32
	prim := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(60 * time.Millisecond) // slower than HedgeAfter so the secondary timer fires
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`from-primary`))
	}))
	defer prim.Close()
	sec := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondaryHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`from-secondary`))
	}))
	defer sec.Close()

	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   time.Minute,
	})
	cm.Record("sec", types.ProtoRPC, false)
	cm.Record("sec", types.ProtoRPC, false) // tripped; cooldown nowhere near elapsed

	fwd := NewHTTP(stubSelector{cands: []*backend.Backend{
		mkBackend("prim", prim.URL),
		mkBackend("sec", sec.URL),
	}}, pool.NewHTTPPool(), cm, Policy{
		MaxAttempts:       3,
		PerAttemptTimeout: 2 * time.Second,
		HedgeAfter:        10 * time.Millisecond,
	})

	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/eth_call", nil)
	fwd.Hedge(rec, r, types.RouteKey{
		Protocol:   types.ProtoRPC,
		Method:     "eth_call",
		Class:      types.ClassByHeight,
		Idempotent: true,
		Hedge:      true,
	})

	if rec.Code != 200 || rec.Body.String() != "from-primary" {
		t.Errorf("primary should serve alone; status %d body=%q", rec.Code, rec.Body.String())
	}
	if secondaryHits.Load() != 0 {
		t.Errorf("tripped secondary must not be dispatched; hits=%d", secondaryHits.Load())
	}
	if st := cm.State("sec", types.ProtoRPC); st != circuit.StateOpen {
		t.Errorf("skipped secondary's breaker must be untouched; state=%s", st)
	}
}

// A tripped first candidate cannot be the primary leg: the next acquirable
// candidate serves instead.
func TestHedgeTrippedPrimaryServesViaFallback(t *testing.T) {
	var firstHits atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`from-first`))
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`from-second`))
	}))
	defer second.Close()

	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   time.Minute,
	})
	cm.Record("first", types.ProtoRPC, false)
	cm.Record("first", types.ProtoRPC, false) // tripped

	fwd := NewHTTP(stubSelector{cands: []*backend.Backend{
		mkBackend("first", first.URL),
		mkBackend("second", second.URL),
	}}, pool.NewHTTPPool(), cm, Policy{
		MaxAttempts:       3,
		PerAttemptTimeout: 2 * time.Second,
		HedgeAfter:        10 * time.Millisecond,
	})

	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/eth_call", nil)
	fwd.Hedge(rec, r, types.RouteKey{
		Protocol:   types.ProtoRPC,
		Method:     "eth_call",
		Class:      types.ClassByHeight,
		Idempotent: true,
		Hedge:      true,
	})

	if rec.Code != 200 || rec.Body.String() != "from-second" {
		t.Errorf("expected the fallback primary to serve; status %d body=%q", rec.Code, rec.Body.String())
	}
	if firstHits.Load() != 0 {
		t.Errorf("tripped candidate must not be dispatched; hits=%d", firstHits.Load())
	}
	if st := cm.State("first", types.ProtoRPC); st != circuit.StateOpen {
		t.Errorf("tripped candidate's breaker must be untouched; state=%s", st)
	}
}

// With every candidate unacquirable, Hedge delegates to Forward's skip and
// exhaustion semantics instead of firing blind.
func TestHedgeAllTrippedDelegatesToForward(t *testing.T) {
	var hits atomic.Int32
	mk := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.WriteHeader(200)
		}))
	}
	a, b := mk(), mk()
	defer a.Close()
	defer b.Close()

	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   time.Minute,
	})
	for _, name := range []string{"ha", "hb"} {
		cm.Record(name, types.ProtoRPC, false)
		cm.Record(name, types.ProtoRPC, false)
	}

	fwd := NewHTTP(stubSelector{cands: []*backend.Backend{
		mkBackend("ha", a.URL),
		mkBackend("hb", b.URL),
	}}, pool.NewHTTPPool(), cm, Policy{
		MaxAttempts:       3,
		PerAttemptTimeout: 2 * time.Second,
		HedgeAfter:        10 * time.Millisecond,
	})

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
		t.Errorf("expected 502 with every breaker open; got %d", rec.Code)
	}
	if hits.Load() != 0 {
		t.Errorf("no backend may be dispatched past an open breaker; hits=%d", hits.Load())
	}
}

// Legs cancelled mid-flight by a vanished client resolve their admission
// neutrally: a half-open primary's canary slot is freed, not failed.
func TestHedgeClientGoneReleasesCanary(t *testing.T) {
	var p1Hits, s2Hits atomic.Int32
	p1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p1Hits.Add(1)
		<-r.Context().Done()
	}))
	defer p1.Close()
	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s2Hits.Add(1)
		<-r.Context().Done()
	}))
	defer s2.Close()

	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   50 * time.Millisecond,
	})
	cm.Record("p1", types.ProtoRPC, false)
	cm.Record("p1", types.ProtoRPC, false) // tripped
	time.Sleep(60 * time.Millisecond)      // cooldown elapses: primary admits as canary

	fwd := NewHTTP(stubSelector{cands: []*backend.Backend{
		mkBackend("p1", p1.URL),
		mkBackend("s2", s2.URL),
	}}, pool.NewHTTPPool(), cm, Policy{
		MaxAttempts:       3,
		PerAttemptTimeout: 5 * time.Second,
		HedgeAfter:        5 * time.Millisecond,
	})

	r := httptest.NewRequest(http.MethodGet, "/eth_call", nil)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		rec := httptest.NewRecorder()
		fwd.Hedge(rec, r.WithContext(ctx), types.RouteKey{
			Protocol:   types.ProtoRPC,
			Method:     "eth_call",
			Class:      types.ClassByHeight,
			Idempotent: true,
			Hedge:      true,
		})
	}()

	deadline := time.After(2 * time.Second)
	for p1Hits.Load() == 0 || s2Hits.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("hedge legs never reached the upstreams")
		case <-time.After(time.Millisecond):
		}
	}
	cancel() // client vanishes with both legs in flight
	<-done

	if st := cm.State("p1", types.ProtoRPC); st != circuit.StateHalfOpen {
		t.Errorf("client-gone canary must not resolve the breaker; state=%s", st)
	}
	if !cm.Acquire("p1", types.ProtoRPC) {
		t.Error("canary slot must be free again after a client-gone release")
	}
	if st := cm.State("s2", types.ProtoRPC); st != circuit.StateClosed {
		t.Errorf("client-gone leg must not be recorded against a closed breaker; state=%s", st)
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
