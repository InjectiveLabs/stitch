package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/types"
)

func TestNormalizeWS(t *testing.T) {
	cases := []struct{ in, want string }{
		{"ws://example:8546", "ws://example:8546"},
		{"wss://example:8546", "wss://example:8546"},
		{"http://example:8546", "ws://example:8546"},
		{"https://example:8546", "wss://example:8546"},
		{"example:8546", "example:8546"}, // pass-through for unknown schemes
	}
	for _, c := range cases {
		if got := normalizeWS(c.in); got != c.want {
			t.Errorf("normalizeWS(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// wsBackendMock emulates an eth_ws backend. After ack-ing eth_subscribe,
// the test drives header emission via emit().
type wsBackendMock struct {
	srv      *httptest.Server
	upgrader websocket.Upgrader

	mu       sync.Mutex
	conns    []*websocket.Conn
	subAcked atomic.Int64 // number of successful subscribes seen
}

func newWSBackendMock(t *testing.T) *wsBackendMock {
	t.Helper()
	m := &wsBackendMock{
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.Close)
	return m
}

func (m *wsBackendMock) URL() string {
	// httptest.Server returns http://; the prober normalizes to ws://.
	return m.srv.URL
}

func (m *wsBackendMock) handle(w http.ResponseWriter, r *http.Request) {
	c, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	m.mu.Lock()
	m.conns = append(m.conns, c)
	m.mu.Unlock()

	for {
		_, msg, err := c.ReadMessage()
		if err != nil {
			return
		}
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(msg, &req); err != nil {
			continue
		}
		if req.Method == "eth_subscribe" {
			ack, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  "0xabc",
			})
			if err := c.WriteMessage(websocket.TextMessage, ack); err != nil {
				return
			}
			m.subAcked.Add(1)
		}
	}
}

// emit sends a newHeads notification with the given block number to the
// most recently accepted connection.
func (m *wsBackendMock) emit(t *testing.T, blockNumber int64) {
	t.Helper()
	notif := fmt.Sprintf(
		`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xabc","result":{"number":"0x%x"}}}`,
		blockNumber,
	)
	m.mu.Lock()
	c := m.conns[len(m.conns)-1]
	m.mu.Unlock()
	if err := c.WriteMessage(websocket.TextMessage, []byte(notif)); err != nil {
		t.Fatalf("emit: %v", err)
	}
}

func (m *wsBackendMock) Close() {
	m.mu.Lock()
	conns := m.conns
	m.conns = nil
	m.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	m.srv.Close()
}

func TestEthWSProberSnapshotOnHeader(t *testing.T) {
	mock := newWSBackendMock(t)

	bs := []*backend.Backend{{
		Name:      "tip",
		Endpoints: map[types.Protocol]string{types.ProtoEthWS: mock.URL()},
	}}
	reg := backend.NewRegistry(bs)
	h := NewRegistry()
	p := NewEthWSProber(reg, h)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Drive a single subscribe + emit. We call the inner loop directly
	// here so the test isn't coupled to Run's reconciliation tick.
	done := make(chan error, 1)
	go func() { done <- p.subscribeAndStream(ctx, "tip", mock.URL()) }()

	// Wait for the subscribe ack to land before emitting.
	deadline := time.After(2 * time.Second)
	for mock.subAcked.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("subscribe never acked")
		case <-time.After(10 * time.Millisecond):
		}
	}

	mock.emit(t, 168_438_923)

	// Poll the registry for the snapshot.
	want := int64(168_438_923)
	deadline = time.After(2 * time.Second)
	for {
		snap, ok := h.Get("tip", types.ProtoEthWS)
		if ok && snap.LatestHeight == want {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("snapshot never landed; got ok=%v snap=%+v", ok, snap)
		case <-time.After(10 * time.Millisecond):
		}
	}

	if got := h.MaxHead(); got != want {
		t.Errorf("MaxHead = %d, want %d", got, want)
	}

	cancel()
	// subscribeAndStream returns when ctx is done OR the conn closes.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscribeAndStream did not exit after ctx cancel")
	}
}

// killOldestConn closes the first WS connection the mock accepted —
// simulating the upstream dropping the stream.
func (m *wsBackendMock) killOldestConn(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	if len(m.conns) == 0 {
		m.mu.Unlock()
		t.Fatal("no connections to kill")
	}
	c := m.conns[0]
	m.mu.Unlock()
	_ = c.Close()
}

func TestEthWSProberReconnectAfterDrop(t *testing.T) {
	mock := newWSBackendMock(t)

	bs := []*backend.Backend{{
		Name:      "tip",
		Endpoints: map[types.Protocol]string{types.ProtoEthWS: mock.URL()},
	}}
	reg := backend.NewRegistry(bs)
	h := NewRegistry()
	p := NewEthWSProber(reg, h)
	// Tighten backoff so the test runs fast.
	p.baseBackoff = 50 * time.Millisecond
	p.maxBackoff = 200 * time.Millisecond
	p.healthyStreamThreshold = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go p.trackOne(ctx, "tip", mock.URL())

	waitForAcks := func(n int64) {
		deadline := time.After(3 * time.Second)
		for mock.subAcked.Load() < n {
			select {
			case <-deadline:
				t.Fatalf("expected %d acks, got %d", n, mock.subAcked.Load())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}

	// First subscribe + first header.
	waitForAcks(1)
	mock.emit(t, 100)

	deadline := time.After(2 * time.Second)
	for h.MaxHead() != 100 {
		select {
		case <-deadline:
			t.Fatalf("MaxHead = %d, want 100", h.MaxHead())
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Drop the first connection; expect a reconnect.
	mock.killOldestConn(t)
	waitForAcks(2)

	// Emit a higher header on the new connection; MaxHead must advance.
	mock.emit(t, 150)
	deadline = time.After(2 * time.Second)
	for h.MaxHead() != 150 {
		select {
		case <-deadline:
			t.Fatalf("MaxHead = %d, want 150", h.MaxHead())
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
}

func TestEthWSProberBackendChurn(t *testing.T) {
	a := newWSBackendMock(t)
	b := newWSBackendMock(t)

	// Start with just "alpha".
	bs := []*backend.Backend{{
		Name:      "alpha",
		Endpoints: map[types.Protocol]string{types.ProtoEthWS: a.URL()},
	}}
	reg := backend.NewRegistry(bs)
	h := NewRegistry()
	p := NewEthWSProber(reg, h)
	p.refresh = 100 * time.Millisecond
	p.baseBackoff = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	// alpha gets subscribed.
	waitAck := func(m *wsBackendMock, want int64) {
		deadline := time.After(2 * time.Second)
		for m.subAcked.Load() < want {
			select {
			case <-deadline:
				t.Fatalf("expected %d acks on mock, got %d", want, m.subAcked.Load())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	waitAck(a, 1)

	// Add "beta" via Set; expect a new tracker within ~refresh.
	reg.Set([]*backend.Backend{
		{Name: "alpha", Endpoints: map[types.Protocol]string{types.ProtoEthWS: a.URL()}},
		{Name: "beta", Endpoints: map[types.Protocol]string{types.ProtoEthWS: b.URL()}},
	})
	waitAck(b, 1)

	// Remove "alpha" via Set. After a reconciliation tick, alpha's tracker
	// must exit; emitting on the killed conn doesn't matter, but a second
	// removal-driven cancel shouldn't reconnect. We assert by closing the
	// remaining alpha conn and confirming subAcked does NOT advance further.
	reg.Set([]*backend.Backend{
		{Name: "beta", Endpoints: map[types.Protocol]string{types.ProtoEthWS: b.URL()}},
	})
	// Give the prober a couple of refresh cycles to notice the removal.
	time.Sleep(300 * time.Millisecond)
	a.killOldestConn(t)
	startAcks := a.subAcked.Load()
	time.Sleep(300 * time.Millisecond)
	if got := a.subAcked.Load(); got > startAcks {
		t.Errorf("alpha tracker still reconnecting after removal: acks %d -> %d", startAcks, got)
	}

	cancel()
}
