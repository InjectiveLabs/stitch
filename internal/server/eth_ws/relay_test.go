package eth_ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/health"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/types"
)

// mockWSUpstream upgrades the connection and echoes every text frame back
// with a backend tag prepended.
type mockWSUpstream struct {
	name        string
	conns       atomic.Int64
	readClosed  atomic.Int64
	srv         *httptest.Server
	upgrader    websocket.Upgrader
	beforeFrame func()
}

func newMockWSUpstream(name string) *mockWSUpstream {
	m := &mockWSUpstream{
		name:     name,
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

func (m *mockWSUpstream) handle(w http.ResponseWriter, r *http.Request) {
	c, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	m.conns.Add(1)
	for {
		mt, msg, err := c.ReadMessage()
		if err != nil {
			m.readClosed.Add(1)
			return
		}
		if m.beforeFrame != nil {
			m.beforeFrame()
		}
		echo := []byte(`{"backend":"` + m.name + `","echo":` + string(msg) + `}`)
		if err := c.WriteMessage(mt, echo); err != nil {
			return
		}
	}
}

func (m *mockWSUpstream) URL() string {
	// httptest.NewServer URLs start http://; gorilla's dialer accepts
	// ws://, so we rewrite at the call site.
	return strings.Replace(m.srv.URL, "http://", "ws://", 1)
}

func (m *mockWSUpstream) Close() { m.srv.Close() }

type wsRig struct {
	front    *Server
	frontT   *httptest.Server
	primary  *mockWSUpstream // first selector pick
	fallback *mockWSUpstream
}

func (r *wsRig) close() {
	r.frontT.Close()
	r.primary.Close()
	r.fallback.Close()
}

// setupWS configures a stitch eth_ws listener with two archive-class
// upstreams so both are eligible for "latest" (which is the only thing
// WS handshakes ever ask for). The selector ranks by weight; primary
// gets a higher weight so the test can assert which one was picked.
func setupWS(t *testing.T) *wsRig {
	t.Helper()
	primary := newMockWSUpstream("primary")
	fallback := newMockWSUpstream("fallback")

	bs := []*backend.Backend{
		{
			Name:      "primary",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    200,
			Endpoints: map[types.Protocol]string{types.ProtoEthWS: primary.URL()},
		},
		{
			Name:      "fallback",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoEthWS: fallback.URL()},
		},
	}
	reg := backend.NewRegistry(bs)
	h := health.NewRegistry()
	for _, bb := range bs {
		h.Update(health.Snapshot{
			Backend: bb.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000,
		})
		h.Update(health.Snapshot{
			Backend: bb.Name, Protocol: types.ProtoEthWS, Healthy: true,
		})
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: 100 * time.Millisecond,
	})
	sel := selector.NewRangeSelector(reg, h, cm, 0)

	srv := New("ignored", sel)
	frontT := httptest.NewServer(srv.Handler())
	return &wsRig{front: srv, frontT: frontT, primary: primary, fallback: fallback}
}

// dial dials the stitch listener as a WebSocket client.
func dial(t *testing.T, baseURL string) *websocket.Conn {
	t.Helper()
	u := strings.Replace(baseURL, "http://", "ws://", 1)
	d := &websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	c, _, err := d.DialContext(context.Background(), u, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestWSRelayEcho(t *testing.T) {
	r := setupWS(t)
	defer r.close()

	c := dial(t, r.frontT.URL)
	defer c.Close()

	for i := 0; i < 3; i++ {
		if err := c.WriteMessage(websocket.TextMessage, []byte(`{"hello":1}`)); err != nil {
			t.Fatal(err)
		}
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, msg, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if !strings.Contains(string(msg), `"backend":"primary"`) {
			t.Errorf("expected primary echo; got %s", msg)
		}
	}
	if r.primary.conns.Load() != 1 {
		t.Errorf("expected exactly 1 upstream conn to primary; got %d", r.primary.conns.Load())
	}
	if r.fallback.conns.Load() != 0 {
		t.Errorf("fallback must not be touched; got %d conns", r.fallback.conns.Load())
	}
}

func TestWSClosesUpstreamWhenClientCloses(t *testing.T) {
	r := setupWS(t)
	defer r.close()

	c := dial(t, r.frontT.URL)
	// Send one frame to make sure the relay is established.
	_ = c.WriteMessage(websocket.TextMessage, []byte(`{"x":1}`))
	_, _, _ = c.ReadMessage()

	// Now close the client; the relay should close the upstream.
	_ = c.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if r.primary.readClosed.Load() == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("expected upstream to observe a close; readClosed=%d", r.primary.readClosed.Load())
}

func TestWSClosesClientWhenUpstreamCloses(t *testing.T) {
	r := setupWS(t)
	defer r.close()

	c := dial(t, r.frontT.URL)
	defer c.Close()

	// Establish.
	_ = c.WriteMessage(websocket.TextMessage, []byte(`{"x":1}`))
	_, _, _ = c.ReadMessage()

	// Stop the upstream; the relay must close the client.
	r.primary.Close()

	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err := c.ReadMessage()
	if err == nil {
		t.Fatal("expected client read to error after upstream closes")
	}
}

func TestWSReturns503WhenNoBackend(t *testing.T) {
	// Build a rig with no eth_ws backend.
	bs := []*backend.Backend{
		{
			Name:      "no-eth",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoRPC: "http://127.0.0.1:1"},
		},
	}
	reg := backend.NewRegistry(bs)
	h := health.NewRegistry()
	h.Update(health.Snapshot{Backend: "no-eth", Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100})
	cm := circuit.NewManager(circuit.Policy{ErrorThreshold: 0.5, MinRequests: 1, OpenDuration: time.Second})
	sel := selector.NewRangeSelector(reg, h, cm, 0)

	frontT := httptest.NewServer(New("ignored", sel).Handler())
	defer frontT.Close()

	resp, err := http.Get(frontT.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503; got %d", resp.StatusCode)
	}
}
