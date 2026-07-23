package integration

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/server/eth_ws"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// TestNoGoroutineLeakAcrossSetupTeardown drives a setup → handful of
// requests → teardown cycle and asserts the goroutine count returns to
// roughly the baseline. A leak in the forwarder, listener, or session
// teardown paths would manifest as a steady-state climb across cycles.
//
// The tolerance is deliberately loose — Go's runtime spawns a few
// transient goroutines (GC sweep, runtime locks, http.idleConn reaper)
// that we don't want to flake on. What we actually catch are leaks of
// O(N) per cycle, which is what regression-prone subscription / stream
// teardown bugs look like.
func TestNoGoroutineLeakAcrossSetupTeardown(t *testing.T) {
	if testing.Short() {
		t.Skip("leak: skipping under -short")
	}

	// One full cycle to warm the runtime: HTTP transport pools, etc.
	doCycle(t)
	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	baseline := runtime.NumGoroutine()
	const cycles = 5
	for i := 0; i < cycles; i++ {
		doCycle(t)
	}
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	runtime.GC() // give finalizers two passes

	final := runtime.NumGoroutine()
	delta := final - baseline
	t.Logf("goroutine baseline=%d final=%d delta=%d (after %d cycles)", baseline, final, delta, cycles)

	// Tolerance: 20 goroutines of slack across 5 cycles. Real leaks would
	// show a slope of N per cycle, blowing past this immediately.
	if delta > 20 {
		t.Errorf("goroutine count grew by %d over %d cycles (likely leak)", delta, cycles)
	}
}

// TestEthWSShutdownClosesLiveSessions covers the hijacked-conn shutdown
// gap: http.Server.Shutdown neither waits for nor closes WebSocket
// connections, so the eth_ws server must track its live sessions and
// force-close them on Shutdown. Asserts:
//
//	(a) Shutdown returns promptly — it must not wait for clients to leave
//	(b) goroutine count returns to baseline (sessions fully unwound)
//	(c) every connected client observes a close
func TestEthWSShutdownClosesLiveSessions(t *testing.T) {
	if testing.Short() {
		t.Skip("leak: skipping under -short")
	}

	up := newWSEthMock(t) // acks eth_subscribe; reuses the head-routing mock

	bs := []*backend.Backend{{
		Name:      "ws-up",
		Coverage:  backend.Coverage{Kind: backend.CovArchive},
		Weight:    100,
		Endpoints: map[types.Protocol]string{types.ProtoEthWS: up.URL()},
	}}
	reg := backend.NewRegistry(bs)
	h := health.NewRegistry()
	h.Update(health.Snapshot{Backend: "ws-up", Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000})
	h.Update(health.Snapshot{Backend: "ws-up", Protocol: types.ProtoEthWS, Healthy: true})
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: 100 * time.Millisecond,
	})
	sel := selector.NewRangeSelector(reg, h, cm, 0)

	srv := eth_ws.New("ignored", sel)
	front := httptest.NewServer(srv.Handler())
	defer front.Close()

	runtime.GC()
	baseline := runtime.NumGoroutine()

	// Connect N clients, each with an active eth_subscribe.
	const clients = 5
	conns := make([]*websocket.Conn, 0, clients)
	d := &websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	for i := 0; i < clients; i++ {
		c, _, err := d.Dial(strings.Replace(front.URL, "http://", "ws://", 1), nil)
		if err != nil {
			t.Fatalf("client %d dial: %v", i, err)
		}
		defer c.Close()
		conns = append(conns, c)
		if err := c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`)); err != nil {
			t.Fatalf("client %d subscribe: %v", i, err)
		}
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, _, err := c.ReadMessage(); err != nil {
			t.Fatalf("client %d subscribe response: %v", i, err)
		}
	}

	// (a) Shutdown returns well before the deadline even though every
	// client is still connected.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Errorf("Shutdown took %v; it must not wait for clients to leave", elapsed)
	}

	// (c) every client observes the close — the next read fails with a
	// close/EOF error, not a deadline timeout (a timeout would mean the
	// server left the conn open and we just gave up waiting).
	for i, c := range conns {
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _, err := c.ReadMessage()
		if err == nil {
			t.Errorf("client %d: expected close after shutdown, got a frame", i)
			continue
		}
		if e, ok := err.(net.Error); ok && e.Timeout() {
			t.Errorf("client %d: conn still open after shutdown (read timed out)", i)
		}
	}

	// (b) goroutine count back to baseline while the clients are STILL
	// connected — the sessions must be gone server-side regardless of the
	// clients hanging on. Same tolerance philosophy as the cycle test
	// above: a broken shutdown leaks ~3 goroutines per session (handler,
	// clientReader, upstreamReader) and blows past the slack.
	final := runtime.NumGoroutine()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		final = runtime.NumGoroutine()
		if final <= baseline+8 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("goroutine baseline=%d final=%d (%d ws sessions)", baseline, final, clients)
	if final > baseline+8 {
		t.Errorf("goroutine count grew from %d to %d after shutdown (leaked sessions)", baseline, final)
	}
}

// doCycle stands up the same rig as the smoke test, drives ~5 requests,
// and tears down. We exercise both the cmt_rpc and cosmos_rest paths so
// any leak in either listener's handler stack would show up.
func doCycle(t *testing.T) {
	rig := setup(t)
	defer rig.close()

	for i := 0; i < 5; i++ {
		resp, err := http.Get(rig.rpc.URL + "/status")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	for i := 0; i < 5; i++ {
		resp, err := http.Get(rig.rest.URL + "/cosmos/auth/v1beta1/params")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}
