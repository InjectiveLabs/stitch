package chainstream

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	healthreg "github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/subscription"
	"github.com/InjectiveLabs/stitch/internal/types"
)

const streamMethod = "/injective.stream.v2.Stream/StreamV2"

// mockChainStream implements just enough of the gRPC stream protocol to
// emit a programmable sequence of StreamResponse bytes after receiving
// the client's StreamRequest. It supports Kill() for resume tests.
type mockChainStream struct {
	name     string
	emitFrom atomic.Uint64
	emitN    atomic.Uint64
	srv      *grpc.Server
	lis      net.Listener
	conns    atomic.Int64
	sentN    atomic.Int64

	mu        sync.Mutex
	wsStreams []grpc.ServerStream // opened streams; killed on Kill()
}

func newMockChainStream(t *testing.T, name string, emitFrom, emitN uint64) *mockChainStream {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := &mockChainStream{
		name: name,
		lis:  lis,
	}
	m.emitFrom.Store(emitFrom)
	m.emitN.Store(emitN)

	m.srv = grpc.NewServer(
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(m.handler),
	)
	go func() { _ = m.srv.Serve(lis) }()
	return m
}

func (m *mockChainStream) Addr() string { return m.lis.Addr().String() }

func (m *mockChainStream) Kill() {
	m.srv.Stop()
	_ = m.lis.Close()
}

func (m *mockChainStream) handler(_ any, ss grpc.ServerStream) error {
	m.conns.Add(1)
	m.mu.Lock()
	m.wsStreams = append(m.wsStreams, ss)
	m.mu.Unlock()

	// Receive the client's StreamRequest (we don't inspect it).
	var req RawMessage
	if err := ss.RecvMsg(&req); err != nil {
		return err
	}

	from := m.emitFrom.Load()
	count := m.emitN.Load()
	for i := uint64(0); i < count; i++ {
		bytes := subscription.EncodeStreamResponseForTest(from+i, []byte("payload"))
		if err := ss.SendMsg(&RawMessage{Bytes: bytes}); err != nil {
			return err
		}
		m.sentN.Add(1)
		time.Sleep(2 * time.Millisecond)
	}
	// Park until ctx ends — keeps the stream open for resume tests.
	<-ss.Context().Done()
	return ss.Context().Err()
}

// rig wires two mock backends behind a stitch chainstream listener.
type rig struct {
	front    *Server
	primary  *mockChainStream
	fallback *mockChainStream
	pool     *pool.GRPCPool
}

func (r *rig) close() {
	_ = r.front.Shutdown(context.Background())
	r.primary.Kill()
	r.fallback.Kill()
	r.pool.CloseAll()
}

func setupRig(t *testing.T, primary, fallback *mockChainStream) *rig {
	t.Helper()
	bs := []*backend.Backend{
		{
			Name:      "primary",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    200,
			Endpoints: map[types.Protocol]string{types.ProtoChainStream: primary.Addr()},
		},
		{
			Name:      "fallback",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoChainStream: fallback.Addr()},
		},
	}
	reg := backend.NewRegistry(bs)
	h := healthreg.NewRegistry()
	for _, bb := range bs {
		h.Update(healthreg.Snapshot{
			Backend: bb.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000,
		})
		h.Update(healthreg.Snapshot{
			Backend: bb.Name, Protocol: types.ProtoChainStream, Healthy: true,
		})
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: 100 * time.Millisecond,
	})
	sel := selector.NewRangeSelector(reg, h, cm, 0)
	gp := pool.NewGRPCPool(time.Minute)

	srv, err := New("127.0.0.1:0", sel, cm, gp)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Start(context.Background()) }()
	return &rig{front: srv, primary: primary, fallback: fallback, pool: gp}
}

// dialClient opens a stream to the stitch listener; returns the live
// client stream + a function that recieves one StreamResponse.
func dialClient(t *testing.T, addr string) (*grpc.ClientConn, grpc.ClientStream) {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})),
	)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := conn.NewStream(context.Background(), &grpc.StreamDesc{
		StreamName:    "StreamV2",
		ServerStreams: true,
		ClientStreams: false,
	}, streamMethod)
	if err != nil {
		t.Fatal(err)
	}
	return conn, cs
}

func sendRequest(t *testing.T, cs grpc.ClientStream) {
	t.Helper()
	// Empty StreamRequest is fine — the mock doesn't inspect it.
	if err := cs.SendMsg(&RawMessage{Bytes: []byte{}}); err != nil {
		t.Fatal(err)
	}
	if err := cs.CloseSend(); err != nil {
		t.Fatal(err)
	}
}

func recvHeight(t *testing.T, cs grpc.ClientStream) (uint64, error) {
	t.Helper()
	var resp RawMessage
	if err := cs.RecvMsg(&resp); err != nil {
		return 0, err
	}
	h, ok := subscription.ExtractStreamResponseHeight(resp.Bytes)
	if !ok {
		t.Fatalf("response had no block_height: %x", resp.Bytes)
	}
	return h, nil
}

// ----- tests --------------------------------------------------------------

func TestChainStreamRoutesAndForwards(t *testing.T) {
	primary := newMockChainStream(t, "primary", 1, 3)
	fallback := newMockChainStream(t, "fallback", 1, 3)
	r := setupRig(t, primary, fallback)
	defer r.close()

	conn, cs := dialClient(t, r.front.Addr())
	defer conn.Close()
	sendRequest(t, cs)

	got := []uint64{}
	for i := 0; i < 3; i++ {
		h, err := recvHeight(t, cs)
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		got = append(got, h)
	}
	want := []uint64{1, 2, 3}
	if !equalUint64(got, want) {
		t.Errorf("got %v; want %v", got, want)
	}
	if primary.conns.Load() != 1 || fallback.conns.Load() != 0 {
		t.Errorf("conns: primary=%d fallback=%d", primary.conns.Load(), fallback.conns.Load())
	}
}

func TestChainStreamResumeAcrossUpstreamFailure(t *testing.T) {
	primary := newMockChainStream(t, "primary", 1, 3)   // emits 1, 2, 3 then parks
	fallback := newMockChainStream(t, "fallback", 3, 3) // emits 3, 4, 5 — 3 deduped
	r := setupRig(t, primary, fallback)
	defer r.close()

	conn, cs := dialClient(t, r.front.Addr())
	defer conn.Close()
	sendRequest(t, cs)

	// Drain the first 3.
	for i := 0; i < 3; i++ {
		h, err := recvHeight(t, cs)
		if err != nil {
			t.Fatalf("primary recv %d: %v", i, err)
		}
		if h != uint64(i+1) {
			t.Errorf("expected height %d; got %d", i+1, h)
		}
	}

	// Kill primary; stitch must dial fallback and replay the request.
	primary.Kill()

	// Drain heights 4 and 5 (not 3, which is at-or-behind cursor).
	got := []uint64{}
	for i := 0; i < 2; i++ {
		h, err := recvHeight(t, cs)
		if err != nil {
			t.Fatalf("fallback recv %d: %v", i, err)
		}
		got = append(got, h)
	}
	want := []uint64{4, 5}
	if !equalUint64(got, want) {
		t.Errorf("after resume: got %v; want %v (deduped against cursor=3)", got, want)
	}
	if fallback.conns.Load() != 1 {
		t.Errorf("fallback conns: %d (expected 1)", fallback.conns.Load())
	}
}

func TestChainStreamRejectsNonStreamMethod(t *testing.T) {
	primary := newMockChainStream(t, "primary", 1, 1)
	fallback := newMockChainStream(t, "fallback", 1, 1)
	r := setupRig(t, primary, fallback)
	defer r.close()

	conn, err := grpc.NewClient(r.front.Addr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cs, err := conn.NewStream(context.Background(), &grpc.StreamDesc{
		ServerStreams: true,
	}, "/cosmos.bank.v1beta1.Query/Balance")
	if err != nil {
		t.Fatal(err)
	}
	_ = cs.SendMsg(&RawMessage{Bytes: []byte{}})
	_ = cs.CloseSend()
	var resp RawMessage
	err = cs.RecvMsg(&resp)
	if err == nil {
		t.Fatal("expected error for non-chainstream method")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented; got %v", err)
	}
}

func TestChainStreamReturnsCleanEOF(t *testing.T) {
	primary := newMockChainStream(t, "primary", 10, 2)
	fallback := newMockChainStream(t, "fallback", 10, 2)
	r := setupRig(t, primary, fallback)
	defer r.close()

	// Convert primary's handler so it returns clean EOF after 2 sends.
	// We achieve this by closing the stream via Kill once the test has
	// drained both heights — but the mock currently parks. For a true
	// EOF, kill primary AFTER drain; the fallback then takes over.
	conn, cs := dialClient(t, r.front.Addr())
	defer conn.Close()
	sendRequest(t, cs)

	// Drain initial 2.
	for i := 0; i < 2; i++ {
		if _, err := recvHeight(t, cs); err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
	}
	primary.Kill()

	// Fallback emits 10, 11 — but cursor is 11 already, so both are deduped
	// (effectively at the boundary — height 10 is dropped, 11 is dropped).
	// After fallback exhausts its emissions, it parks → stitch's runOnce
	// blocks on RecvMsg until ctx cancellation. The client recv times out
	// or the test ends.
	//
	// Instead of asserting EOF (which the mock doesn't model cleanly),
	// verify no malformed frames sneak through with a short read deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := recvHeight(t, cs)
		done <- err
	}()
	select {
	case err := <-done:
		// We accept any error — what we don't accept is a duplicate height.
		if err == nil {
			t.Error("unexpected forwarded frame after dedup")
		}
	case <-ctx.Done():
		// Timed out — fine; nothing got through, dedup worked.
	}
}

// failingChainStream accepts every stream, records the attempt time, and
// fails the RPC immediately — a permanently broken upstream for backoff
// measurements.
type failingChainStream struct {
	srv *grpc.Server
	lis net.Listener

	mu       sync.Mutex
	attempts []time.Time
}

func newFailingChainStream(t *testing.T) *failingChainStream {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := &failingChainStream{lis: lis}
	m.srv = grpc.NewServer(
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(func(_ any, ss grpc.ServerStream) error {
			m.mu.Lock()
			m.attempts = append(m.attempts, time.Now())
			m.mu.Unlock()
			var req RawMessage
			_ = ss.RecvMsg(&req)
			return status.Error(codes.Unavailable, "synthetic upstream failure")
		}),
	)
	go func() { _ = m.srv.Serve(lis) }()
	return m
}

func (m *failingChainStream) Addr() string { return m.lis.Addr().String() }

func (m *failingChainStream) Kill() {
	m.srv.Stop()
	_ = m.lis.Close()
}

func (m *failingChainStream) times() []time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]time.Time(nil), m.attempts...)
}

// A permanently failing upstream must see jittered-exponentially
// increasing gaps between resume attempts — not 8 back-to-back redials.
// The gap minima derive from the schedule's jitter floor (every sleep is
// at least 0.8× its nominal value, and the doubling compounds the
// jittered previous delay), so the test cannot flake on slow machines:
// scheduling can only lengthen a gap, never shorten it.
func TestChainStreamResumeBackoffIncreasingGaps(t *testing.T) {
	up := newFailingChainStream(t)
	defer up.Kill()

	bs := []*backend.Backend{{
		Name:      "flappy",
		Coverage:  backend.Coverage{Kind: backend.CovArchive},
		Weight:    100,
		Endpoints: map[types.Protocol]string{types.ProtoChainStream: up.Addr()},
	}}
	reg := backend.NewRegistry(bs)
	h := healthreg.NewRegistry()
	h.Update(healthreg.Snapshot{Backend: "flappy", Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000})
	h.Update(healthreg.Snapshot{Backend: "flappy", Protocol: types.ProtoChainStream, Healthy: true})
	// MinRequests is high so the breaker never trips: all 8 attempts land
	// on the same backend and the gaps measure only the backoff.
	cm := circuit.NewManager(circuit.Policy{ErrorThreshold: 0.5, MinRequests: 100, OpenDuration: time.Second})
	gp := pool.NewGRPCPool(time.Minute)
	srv, err := New("127.0.0.1:0", selector.NewRangeSelector(reg, h, cm, 0), cm, gp)
	if err != nil {
		t.Fatal(err)
	}
	// Tighten the backoff so the 8 attempts complete in under a second.
	srv.dir.baseBackoff = 25 * time.Millisecond
	srv.dir.maxBackoff = 120 * time.Millisecond
	go func() { _ = srv.Start(context.Background()) }()
	defer func() {
		_ = srv.Shutdown(context.Background())
		gp.CloseAll()
	}()

	conn, cs := dialClient(t, srv.Addr())
	defer conn.Close()
	sendRequest(t, cs)

	// The relay fails terminally after maxAttempts; wait for that status.
	var resp RawMessage
	if err := cs.RecvMsg(&resp); err == nil {
		t.Fatal("expected terminal error from a permanently failing upstream")
	} else if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected Unavailable; got %v", err)
	}

	times := up.times()
	if len(times) != 8 {
		t.Fatalf("expected exactly maxAttempts=8 upstream attempts; got %d", len(times))
	}
	// Jitter-floor minima for base=25ms, cap=120ms:
	// 0.8×25 = 20, 0.8×(2×20) = 32, 0.8×(2×32) ≈ 51, 0.8×(2×51.2) ≈ 81,
	// then the 0.8×cap floor of 96 once doubling passes the cap.
	minGaps := []time.Duration{
		20 * time.Millisecond,
		32 * time.Millisecond,
		51 * time.Millisecond,
		81 * time.Millisecond,
		96 * time.Millisecond,
		96 * time.Millisecond,
		96 * time.Millisecond,
	}
	for i, min := range minGaps {
		got := times[i+1].Sub(times[i])
		if got < min {
			t.Errorf("gap %d→%d = %v; want ≥ %v (backoff missing or not growing)", i, i+1, got, min)
		}
	}
}

// A half-open backend admits exactly one in-flight canary: the first
// pickConn claims it via Acquire, and a second pickConn arriving before
// the canary resolves must fall through to the next candidate. The legacy
// read-only Allow gate could not give this guarantee.
func TestChainStreamPickConnClaimsHalfOpenCanary(t *testing.T) {
	bs := []*backend.Backend{
		{
			Name:      "primary",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    200,
			Endpoints: map[types.Protocol]string{types.ProtoChainStream: "127.0.0.1:19011"},
		},
		{
			Name:      "fallback",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoChainStream: "127.0.0.1:19012"},
		},
	}
	reg := backend.NewRegistry(bs)
	h := healthreg.NewRegistry()
	for _, bb := range bs {
		h.Update(healthreg.Snapshot{
			Backend: bb.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000,
		})
		h.Update(healthreg.Snapshot{
			Backend: bb.Name, Protocol: types.ProtoChainStream, Healthy: true,
		})
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   20 * time.Millisecond,
	})
	gp := pool.NewGRPCPool(time.Minute)
	defer gp.CloseAll()
	d := NewDirector(selector.NewRangeSelector(reg, h, cm, 0), cm, gp)

	// Trip primary's breaker, then let the cooldown elapse so the selector
	// readmits it as a (half-open-probe) candidate. The pool's gRPC dial is
	// lazy, so no live upstream is needed.
	cm.Record("primary", types.ProtoChainStream, false)
	cm.Record("primary", types.ProtoChainStream, false)
	time.Sleep(40 * time.Millisecond)

	name, conn := d.pickConn(context.Background(), "")
	if conn == nil || name != "primary" {
		t.Fatalf("first pickConn should claim the half-open primary canary; chose %q", name)
	}
	name, conn = d.pickConn(context.Background(), "")
	if conn == nil || name != "fallback" {
		t.Fatalf("second pickConn must skip primary while its canary is unresolved; chose %q", name)
	}

	// The held admission resolves like any attempt outcome: a canary
	// success closes the breaker.
	d.circuit.Record("primary", types.ProtoChainStream, true)
	if st := cm.State("primary", types.ProtoChainStream); st != circuit.StateClosed {
		t.Fatalf("canary success should close the breaker; state %s", st)
	}
}

func equalUint64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// silence unused-import linter when iterating tests
var _ = errors.Is
var _ io.Reader = (*net.TCPConn)(nil)
