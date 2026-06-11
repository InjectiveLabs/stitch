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

// mcMock is injMock with a configurable delay between the subscribe ack
// and the first notification, so a multicast test can attach N clients
// before emission starts (the hub fans out only to already-attached
// subscribers).
type mcMock struct {
	emitFrom  atomic.Int64
	emitN     atomic.Int64
	emitDelay time.Duration
	srv       *httptest.Server
	upgrader  websocket.Upgrader
	conns     atomic.Int64

	mu      sync.Mutex
	wsConns []*websocket.Conn
}

func newMCMock(t *testing.T, emitFrom, emitN int64, emitDelay time.Duration) *mcMock {
	t.Helper()
	m := &mcMock{
		emitDelay: emitDelay,
		upgrader:  websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
	m.emitFrom.Store(emitFrom)
	m.emitN.Store(emitN)
	mux := http.NewServeMux()
	mux.HandleFunc("/injstream-ws", m.handle)
	m.srv = httptest.NewServer(mux)
	t.Cleanup(m.Kill)
	return m
}

func (m *mcMock) HostPort() string { return strings.TrimPrefix(m.srv.URL, "http://") }

func (m *mcMock) Kill() {
	m.mu.Lock()
	conns := m.wsConns
	m.wsConns = nil
	m.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	m.srv.Listener.Close()
}

func (m *mcMock) handle(w http.ResponseWriter, r *http.Request) {
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
	case <-time.After(3 * time.Second):
		return
	case <-readDone:
		return
	}

	if m.emitDelay > 0 {
		select {
		case <-time.After(m.emitDelay):
		case <-readDone:
			return
		}
	}
	from := m.emitFrom.Load()
	count := m.emitN.Load()
	for i := int64(0); i < count; i++ {
		notif := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"block_height":%d}}`, string(subID), from+i)
		if err := writeFrame([]byte(notif)); err != nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	<-readDone
}

// setupMulticastRig builds an inj_ws server in multicast mode over the
// given upstreams (descending weight ⇒ deterministic preference order).
func setupMulticastRig(t *testing.T, opts SubscriptionOptions, mocks ...*mcMock) (*Server, *httptest.Server) {
	t.Helper()
	bs := make([]*backend.Backend, len(mocks))
	for i, m := range mocks {
		bs[i] = &backend.Backend{
			Name:      fmt.Sprintf("mc-u%d", i),
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    200 - i,
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

	srv := New("ignored", sel)
	opts.Multicast = true
	srv.SetSubscriptions(opts)
	front := httptest.NewServer(srv.Handler())
	t.Cleanup(front.Close)
	return srv, front
}

// TestInjWSMulticastE2EResumeNoDuplicates is the headline multicast test:
// N clients with the same canonical filter share ONE upstream connection;
// killing that upstream resumes onto the fallback and nobody sees a
// duplicate or a gap.
func TestInjWSMulticastE2EResumeNoDuplicates(t *testing.T) {
	primary := newMCMock(t, 1, 3, 300*time.Millisecond)  // emits 1,2,3 after all clients attach
	fallback := newMCMock(t, 3, 3, 200*time.Millisecond) // emits 3,4,5 — 3 deduped by the shared cursor
	srv, front := setupMulticastRig(t, SubscriptionOptions{ReplayTimeout: 5 * time.Second}, primary, fallback)

	const N = 5
	clients := make([]*websocket.Conn, N)
	for i := range clients {
		c := dial(t, front.URL)
		defer c.Close()
		clients[i] = c
		// Same filter — spacing in subscription_id/JSON-RPC id must not
		// defeat canonical-key coalescing.
		req := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"subscribe","params":{"subscription_id":"client-%d","filter":{"oracle_price_filter":{"symbol":["BTC","ETH"]}}}}`, 100+i, i)
		if err := c.WriteMessage(websocket.TextMessage, []byte(req)); err != nil {
			t.Fatal(err)
		}
		var ack struct {
			ID     int    `json:"id"`
			Result string `json:"result"`
		}
		readJSON(t, c, &ack)
		if ack.Result != "success" || ack.ID != 100+i {
			t.Fatalf("client %d subscribe ack: %+v", i, ack)
		}
	}

	type notifBody struct {
		ID     int `json:"id"`
		Result struct {
			BlockHeight uint64 `json:"block_height"`
		} `json:"result"`
	}
	readHeights := func(c *websocket.Conn, want int, clientIdx int) []uint64 {
		heights := make([]uint64, 0, want)
		for len(heights) < want {
			var n notifBody
			readJSON(t, c, &n)
			if n.ID != 100+clientIdx {
				t.Errorf("client %d: notification id=%d; want %d (its own)", clientIdx, n.ID, 100+clientIdx)
			}
			heights = append(heights, n.Result.BlockHeight)
		}
		return heights
	}

	all := make([][]uint64, N)
	for i, c := range clients {
		all[i] = readHeights(c, 3, i)
	}
	if got := primary.conns.Load(); got != 1 {
		t.Errorf("primary conns = %d; want 1 (multicast: N clients share one upstream)", got)
	}
	if srv.hub.UpstreamCount() != 1 {
		t.Errorf("hub upstream count = %d; want 1", srv.hub.UpstreamCount())
	}

	primary.Kill()

	for i, c := range clients {
		all[i] = append(all[i], readHeights(c, 2, i)...)
	}
	for i, heights := range all {
		if !equalUint64(heights, []uint64{1, 2, 3, 4, 5}) {
			t.Errorf("client %d heights = %v; want [1 2 3 4 5] (no duplicates, no gaps)", i, heights)
		}
	}
	if got := fallback.conns.Load(); got != 1 {
		t.Errorf("fallback conns = %d; want 1 (single resume for all clients)", got)
	}

	// No trailing duplicates: the streams are drained, so the next read
	// must time out rather than produce a frame.
	c := clients[0]
	_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, _, err := c.ReadMessage(); err == nil {
		t.Error("client 0 received an extra frame after the expected 5 heights")
	}
}

// TestInjWSMulticastRejectsNonSubscribeFrames pins the documented design
// decision: multicast mode has no per-client upstream, so passthrough
// JSON-RPC gets a -32601 error instead of being forwarded.
func TestInjWSMulticastRejectsNonSubscribeFrames(t *testing.T) {
	up := newMCMock(t, 1, 0, 0)
	_, front := setupMulticastRig(t, SubscriptionOptions{}, up)

	c := dial(t, front.URL)
	defer c.Close()

	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":5,"method":"eth_blockNumber"}`)); err != nil {
		t.Fatal(err)
	}
	var resp struct {
		ID    int `json:"id"`
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	readJSON(t, c, &resp)
	if resp.Error.Code != -32601 || resp.ID != 5 {
		t.Errorf("expected -32601 with the request's id; got %+v", resp)
	}
	if !strings.Contains(resp.Error.Message, "multicast") {
		t.Errorf("error message should name multicast mode; got %q", resp.Error.Message)
	}
}

// TestInjWSMulticastUnsubscribeReleasesUpstream: unsubscribe replies
// success and the hub tears the upstream down once its last subscriber
// is gone.
func TestInjWSMulticastUnsubscribeReleasesUpstream(t *testing.T) {
	up := newMCMock(t, 1, 0, 0) // quiet: teardown must not need a frame to notice
	srv, front := setupMulticastRig(t, SubscriptionOptions{}, up)

	c := dial(t, front.URL)
	defer c.Close()

	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"subscription_id":"u1","filter":{}}}`)); err != nil {
		t.Fatal(err)
	}
	var ack struct {
		Result string `json:"result"`
	}
	readJSON(t, c, &ack)
	if ack.Result != "success" {
		t.Fatalf("subscribe ack: %+v", ack)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && srv.hub.UpstreamCount() == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":2,"method":"unsubscribe","params":{"subscription_id":"u1"}}`)); err != nil {
		t.Fatal(err)
	}
	var unsub struct {
		ID     int    `json:"id"`
		Result string `json:"result"`
	}
	readJSON(t, c, &unsub)
	if unsub.Result != "success" || unsub.ID != 2 {
		t.Errorf("unsubscribe reply: %+v", unsub)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.hub.UpstreamCount() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("hub upstream still alive after last unsubscribe; count=%d", srv.hub.UpstreamCount())
}

// TestInjWSMulticastShutdown mirrors the 1:1 shutdown test: Shutdown
// must promptly close hijacked client conns AND the hub's upstreams.
func TestInjWSMulticastShutdown(t *testing.T) {
	up := newMCMock(t, 1, 0, 0) // quiet conns are the worst case for teardown
	srv, front := setupMulticastRig(t, SubscriptionOptions{}, up)

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
		t.Errorf("Shutdown took %v; it must not wait for clients or upstream deadlines", elapsed)
	}
	if n := srv.hub.UpstreamCount(); n != 0 {
		t.Errorf("hub upstreams after shutdown: %d", n)
	}

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := c.ReadMessage()
	if err == nil {
		t.Fatal("expected close after shutdown, got a frame")
	}
	if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Errorf("conn still open after shutdown (read timed out)")
	}
}
