package subscription

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// hubMock wraps a /injstream-ws-shaped upstream that emits N notifications
// after a subscribe, and counts how many distinct connections it has
// served. The chunk that proves multicast is `conns.Load() == 1`.
type hubMock struct {
	emitN    int64
	emitGate chan struct{} // emission waits for this to close (newHubMock pre-closes it)
	srv      *httptest.Server
	upgrader websocket.Upgrader
	conns    atomic.Int64

	mu      sync.Mutex
	wsConns []*websocket.Conn
}

func newHubMock(t *testing.T, emitN int64) *hubMock {
	t.Helper()
	m := newGatedHubMock(t, emitN)
	close(m.emitGate) // emit as soon as the subscribe ack is out
	return m
}

// newGatedHubMock holds emission until the test closes emitGate, so the
// test can attach every subscriber before the first event — the hub fans
// out only to already-attached subscribers, so an ungated mock races
// late attachers against its emit window.
func newGatedHubMock(t *testing.T, emitN int64) *hubMock {
	t.Helper()
	m := &hubMock{
		emitN:    emitN,
		emitGate: make(chan struct{}),
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/injstream-ws", m.handle)
	m.srv = httptest.NewServer(mux)
	t.Cleanup(m.Close)
	return m
}

func (m *hubMock) HostPort() string {
	return strings.TrimPrefix(m.srv.URL, "http://")
}

func (m *hubMock) Close() {
	m.mu.Lock()
	conns := m.wsConns
	m.wsConns = nil
	m.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	m.srv.Close()
}

func (m *hubMock) handle(w http.ResponseWriter, r *http.Request) {
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
	readDone := make(chan struct{})
	// Single reader goroutine — gorilla forbids concurrent readers.
	// Reads from c throughout the handler's lifetime; emit-then-park
	// happens via writes only.
	var writeMu sync.Mutex
	writeFrame := func(data []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return c.WriteMessage(websocket.TextMessage, data)
	}
	go func() {
		defer close(readDone)
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
				_ = writeFrame(ack)
				select {
				case subReady <- probe.ID:
				default:
				}
			}
		}
	}()

	var subID json.RawMessage
	select {
	case subID = <-subReady:
	case <-time.After(2 * time.Second):
		return
	case <-readDone:
		return
	}

	select {
	case <-m.emitGate:
	case <-readDone:
		return
	}

	for i := int64(1); i <= m.emitN; i++ {
		notif := fmt.Sprintf(
			`{"jsonrpc":"2.0","id":%s,"result":{"block_height":%d}}`, string(subID), i,
		)
		if err := writeFrame([]byte(notif)); err != nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	// Wait for the reader to exit (peer close) — single reader, no race.
	<-readDone
}

func newHubRig(t *testing.T, mocks ...*hubMock) *Hub {
	t.Helper()
	bs := make([]*backend.Backend, len(mocks))
	for i, m := range mocks {
		bs[i] = &backend.Backend{
			Name:      fmt.Sprintf("u%d", i),
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100 + i, // tiebreak deterministic
			Endpoints: map[types.Protocol]string{types.ProtoChainStream: m.HostPort()},
		}
	}
	reg := backend.NewRegistry(bs)
	h := health.NewRegistry()
	for _, b := range bs {
		h.Update(health.Snapshot{Backend: b.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000})
		h.Update(health.Snapshot{Backend: b.Name, Protocol: types.ProtoChainStream, Healthy: true})
	}
	cm := circuit.NewManager(circuit.Policy{ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: 100 * time.Millisecond})
	sel := selector.NewRangeSelector(reg, h, cm, 0)
	return NewHub(sel, &websocket.Dialer{HandshakeTimeout: 2 * time.Second})
}

// TestHubMulticast100Clients verifies that 100 client subscriptions to
// the same canonical filter end up sharing a single upstream connection.
func TestHubMulticast100Clients(t *testing.T) {
	// Gated mock: emission starts only after every subscriber is attached.
	// Ungated, the 10ms emit window raced the attach loop and a scheduling
	// stall (e.g. under -race) orphaned the late tail of subscribers.
	mock := newGatedHubMock(t, 5)
	hub := newHubRig(t, mock)

	const N = 100
	subs := make([]*Subscriber, N)
	for i := 0; i < N; i++ {
		filter := json.RawMessage(`{"oracle_price_filter":{"symbol":["BTC","ETH"]}}`)
		s, err := hub.Subscribe(context.Background(), filter, fmt.Sprintf("client-%d", i), json.RawMessage(fmt.Sprintf("%d", i)))
		if err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
		subs[i] = s
	}
	defer func() {
		for _, s := range subs {
			s.Detach()
		}
	}()
	// Subscribe attaches synchronously, so all N subscribers are in the
	// fan-out list — every one of them must now see every event.
	close(mock.emitGate)

	// Wait for first event on every subscriber.
	deadline := time.Now().Add(3 * time.Second)
	received := make([]int, N)
	for i, s := range subs {
		for received[i] == 0 && time.Now().Before(deadline) {
			select {
			case msg := <-s.Out:
				if !strings.Contains(string(msg), `"id":`) {
					t.Errorf("subscriber %d: unexpected message %s", i, msg)
				}
				received[i] = 1
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	for i, r := range received {
		if r == 0 {
			t.Errorf("subscriber %d never received an event", i)
		}
	}
	if mock.conns.Load() != 1 {
		t.Errorf("expected exactly 1 upstream connection (multicast); got %d", mock.conns.Load())
	}
	if hub.UpstreamCount() != 1 {
		t.Errorf("expected 1 upstream entry in hub; got %d", hub.UpstreamCount())
	}
}

// TestHubDifferentFiltersDifferentUpstreams: clients with semantically
// different filters get their own upstreams.
func TestHubDifferentFiltersDifferentUpstreams(t *testing.T) {
	mock := newHubMock(t, 1)
	hub := newHubRig(t, mock)

	a, err := hub.Subscribe(context.Background(), json.RawMessage(`{"a":1}`), "ca", json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Detach()
	b, err := hub.Subscribe(context.Background(), json.RawMessage(`{"a":2}`), "cb", json.RawMessage(`2`))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Detach()

	// Allow upstream dials to complete.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && mock.conns.Load() < 2 {
		time.Sleep(20 * time.Millisecond)
	}
	if mock.conns.Load() != 2 {
		t.Errorf("expected 2 upstream connections (one per filter); got %d", mock.conns.Load())
	}
}

// TestHubSlowConsumerDropsOldest: when a single subscriber's send buffer
// fills, the hub drops oldest events for that client only — other
// clients on the same upstream keep receiving.
func TestHubSlowConsumerDropsOldest(t *testing.T) {
	mock := newHubMock(t, 200) // emit a lot
	hub := newHubRig(t, mock)
	hub.SlowConsumer = "drop"
	hub.SendBufSize = 4 // tiny buffer to force drops

	// Subscribe twice to share an upstream.
	slow, err := hub.Subscribe(context.Background(), json.RawMessage(`{"x":1}`), "slow", json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Detach()
	fast, err := hub.Subscribe(context.Background(), json.RawMessage(`{"x":1}`), "fast", json.RawMessage(`2`))
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Detach()

	// "Slow" never reads from Out. "Fast" reads aggressively.
	fastReads := atomic.Int64{}
	go func() {
		for {
			select {
			case <-fast.Out:
				fastReads.Add(1)
			case <-fast.Done():
				return
			}
		}
	}()

	// Let the upstream emit for a bit.
	time.Sleep(800 * time.Millisecond)

	if slow.DropCount() == 0 {
		t.Errorf("slow consumer should accumulate drops; got %d", slow.DropCount())
	}
	if fastReads.Load() < 50 {
		t.Errorf("fast consumer should have received many events; got %d", fastReads.Load())
	}
}

// TestHubSubscribeTearsDownOnLastDetach: the upstream connection closes
// when the last subscriber detaches.
func TestHubSubscribeTearsDownOnLastDetach(t *testing.T) {
	mock := newHubMock(t, 5)
	hub := newHubRig(t, mock)

	a, err := hub.Subscribe(context.Background(), json.RawMessage(`{"k":1}`), "a", json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	if hub.UpstreamCount() != 1 {
		t.Fatalf("upstream count: %d", hub.UpstreamCount())
	}
	a.Detach()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.UpstreamCount() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("hub did not tear down upstream after last detach; UpstreamCount=%d", hub.UpstreamCount())
}
