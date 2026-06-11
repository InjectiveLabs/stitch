package inj_ws

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// injMock is a programmable /injstream-ws upstream. After receiving a
// `subscribe` request, it ACKs with `"success"` and emits N notifications
// using the same JSON-RPC id and incrementing block_height values.
type injMock struct {
	name     string
	emitFrom atomic.Int64
	emitN    atomic.Int64
	srv      *httptest.Server
	upgrader websocket.Upgrader
	conns    atomic.Int64

	mu      sync.Mutex
	wsConns []*websocket.Conn
}

func newInjMock(name string, emitFrom, emitN int64) *injMock {
	m := &injMock{
		name:     name,
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
	m.emitFrom.Store(emitFrom)
	m.emitN.Store(emitN)
	mux := http.NewServeMux()
	mux.HandleFunc("/injstream-ws", m.handle)
	m.srv = httptest.NewServer(mux)
	return m
}

func (m *injMock) URL() string {
	return strings.Replace(m.srv.URL, "http://", "ws://", 1) + "/injstream-ws"
}

// HostPort returns the bare host:port form so the operator can pass that
// to stitch's chainstream endpoint config.
func (m *injMock) HostPort() string {
	return strings.TrimPrefix(m.srv.URL, "http://")
}

func (m *injMock) Kill() {
	m.mu.Lock()
	conns := m.wsConns
	m.wsConns = nil
	m.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	m.srv.Listener.Close()
}

func (m *injMock) handle(w http.ResponseWriter, r *http.Request) {
	c, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	m.conns.Add(1)
	m.mu.Lock()
	m.wsConns = append(m.wsConns, c)
	m.mu.Unlock()

	subReady := make(chan json.RawMessage, 1)
	var once sync.Once
	subDone := make(chan struct{})

	go func() {
		defer close(subDone)
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			var probe struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			_ = json.Unmarshal(msg, &probe)
			if probe.Method == "subscribe" {
				ack, _ := json.Marshal(map[string]any{
					"jsonrpc": "2.0",
					"id":      json.RawMessage(probe.ID),
					"result":  "success",
				})
				_ = c.WriteMessage(websocket.TextMessage, ack)
				once.Do(func() { subReady <- probe.ID })
			}
		}
	}()

	var subscribeID json.RawMessage
	select {
	case subscribeID = <-subReady:
	case <-time.After(2 * time.Second):
		return
	}

	from := m.emitFrom.Load()
	count := m.emitN.Load()
	for i := int64(0); i < count; i++ {
		notif := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"block_height":%d,"block_time":%d}}`,
			string(subscribeID), from+i, time.Now().UnixMilli())
		if err := c.WriteMessage(websocket.TextMessage, []byte(notif)); err != nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	<-subDone
}

func setupInjRig(t *testing.T, primary, fallback *injMock) (*Server, *httptest.Server, func()) {
	t.Helper()
	bs := []*backend.Backend{
		{
			Name:      "primary",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    200,
			Endpoints: map[types.Protocol]string{types.ProtoChainStream: primary.HostPort()},
		},
		{
			Name:      "fallback",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoChainStream: fallback.HostPort()},
		},
	}
	reg := backend.NewRegistry(bs)
	h := health.NewRegistry()
	for _, bb := range bs {
		h.Update(health.Snapshot{Backend: bb.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000})
		h.Update(health.Snapshot{Backend: bb.Name, Protocol: types.ProtoChainStream, Healthy: true})
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: 100 * time.Millisecond,
	})
	sel := selector.NewRangeSelector(reg, h, cm, 0)

	srv := New("ignored", sel)
	front := httptest.NewServer(srv.Handler())
	cleanup := func() {
		front.Close()
	}
	return srv, front, cleanup
}

// dial opens a /injstream-ws client to the stitch listener.
func dial(t *testing.T, base string) *websocket.Conn {
	t.Helper()
	u := strings.Replace(base, "http://", "ws://", 1) + EndpointPath
	d := &websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	c, _, err := d.Dial(u, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// readJSONOf reads a single text frame and unmarshals into v.
func readJSON(t *testing.T, c *websocket.Conn, v any) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(msg, v); err != nil {
		t.Fatalf("decode: %v body=%s", err, msg)
	}
}

func TestInjWSSubscribeAndForward(t *testing.T) {
	primary := newInjMock("primary", 1, 3)
	fallback := newInjMock("fallback", 1, 3)
	_, front, cleanup := setupInjRig(t, primary, fallback)
	defer cleanup()
	defer fallback.Kill()
	defer primary.Kill()

	c := dial(t, front.URL)
	defer c.Close()

	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":7,"method":"subscribe","params":{"subscription_id":"abc","filter":{}}}`)); err != nil {
		t.Fatal(err)
	}

	// Read the subscribe ack.
	var ack struct {
		ID     int    `json:"id"`
		Result string `json:"result"`
	}
	readJSON(t, c, &ack)
	if ack.Result != "success" || ack.ID != 7 {
		t.Errorf("subscribe ack: %+v", ack)
	}

	// Read 3 notifications.
	type notifBody struct {
		ID     int `json:"id"`
		Result struct {
			BlockHeight uint64 `json:"block_height"`
		} `json:"result"`
	}
	heights := []uint64{}
	for i := 0; i < 3; i++ {
		var n notifBody
		readJSON(t, c, &n)
		if n.ID != 7 {
			t.Errorf("notification %d: id=%d (expected 7 — client's original)", i, n.ID)
		}
		heights = append(heights, n.Result.BlockHeight)
	}
	if !equalUint64(heights, []uint64{1, 2, 3}) {
		t.Errorf("heights: %v", heights)
	}
}

func TestInjWSResumeAcrossUpstreamFailure(t *testing.T) {
	primary := newInjMock("primary", 1, 3)
	fallback := newInjMock("fallback", 3, 3) // emits 3, 4, 5 — 3 deduped
	_, front, cleanup := setupInjRig(t, primary, fallback)
	defer cleanup()
	defer fallback.Kill()

	c := dial(t, front.URL)
	defer c.Close()

	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":42,"method":"subscribe","params":{"subscription_id":"xyz","filter":{}}}`)); err != nil {
		t.Fatal(err)
	}

	var ack struct {
		ID     int    `json:"id"`
		Result string `json:"result"`
	}
	readJSON(t, c, &ack)
	if ack.Result != "success" {
		t.Fatalf("ack: %+v", ack)
	}

	type notifBody struct {
		ID     int `json:"id"`
		Result struct {
			BlockHeight uint64 `json:"block_height"`
		} `json:"result"`
	}

	// Drain the first 3.
	heights := []uint64{}
	for i := 0; i < 3; i++ {
		var n notifBody
		readJSON(t, c, &n)
		if n.ID != 42 {
			t.Errorf("primary notification id: %d (expected 42)", n.ID)
		}
		heights = append(heights, n.Result.BlockHeight)
	}

	// Kill primary.
	primary.Kill()

	// Drain heights 4 and 5 from fallback (3 should be deduped).
	for i := 0; i < 2; i++ {
		var n notifBody
		readJSON(t, c, &n)
		if n.ID != 42 {
			t.Errorf("post-resume id: %d (expected 42 — same as before resume)", n.ID)
		}
		heights = append(heights, n.Result.BlockHeight)
	}

	want := []uint64{1, 2, 3, 4, 5}
	if !equalUint64(heights, want) {
		t.Errorf("heights = %v; want %v", heights, want)
	}
	if fallback.conns.Load() != 1 {
		t.Errorf("fallback conns: %d (expected 1 from resume)", fallback.conns.Load())
	}
}

func TestInjWSRejectsBadPath(t *testing.T) {
	primary := newInjMock("primary", 1, 1)
	defer primary.Kill()
	fallback := newInjMock("fallback", 1, 1)
	defer fallback.Kill()
	_, front, cleanup := setupInjRig(t, primary, fallback)
	defer cleanup()

	resp, err := http.Get(front.URL + "/wrong-path")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404; got %d", resp.StatusCode)
	}
}

func TestInjWSRejectsSubscribeWithoutID(t *testing.T) {
	primary := newInjMock("primary", 1, 1)
	defer primary.Kill()
	fallback := newInjMock("fallback", 1, 1)
	defer fallback.Kill()
	_, front, cleanup := setupInjRig(t, primary, fallback)
	defer cleanup()

	c := dial(t, front.URL)
	defer c.Close()

	// Missing subscription_id.
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"filter":{}}}`)); err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Error struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	readJSON(t, c, &resp)
	if resp.Error.Code != -32602 {
		t.Errorf("expected -32602 InvalidParams; got %d", resp.Error.Code)
	}
}

// TestInjWSShutdownClosesLiveSessions: Shutdown must force-close live
// (hijacked) WS sessions — http.Server.Shutdown alone would leave them
// running until the client disconnects on its own.
func TestInjWSShutdownClosesLiveSessions(t *testing.T) {
	primary := newInjMock("primary", 1, 0) // acks subscribe, emits nothing
	defer primary.Kill()
	fallback := newInjMock("fallback", 1, 0)
	defer fallback.Kill()
	srv, front, cleanup := setupInjRig(t, primary, fallback)
	defer cleanup()

	c := dial(t, front.URL)
	defer c.Close()
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":9,"method":"subscribe","params":{"subscription_id":"shut","filter":{}}}`)); err != nil {
		t.Fatal(err)
	}
	var ack struct {
		Result string `json:"result"`
	}
	readJSON(t, c, &ack)
	if ack.Result != "success" {
		t.Fatalf("ack: %+v", ack)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Errorf("Shutdown took %v; it must not wait for the client to leave", elapsed)
	}

	// The next read must fail with a close/EOF error, not a deadline
	// timeout — a timeout would mean the conn was simply left open.
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := c.ReadMessage()
	if err == nil {
		t.Fatal("expected close after shutdown, got a frame")
	}
	if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Errorf("conn still open after shutdown (read timed out)")
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
