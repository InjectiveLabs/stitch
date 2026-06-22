package eth_ws

import (
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

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/health"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/types"
)

// resumeMock is a programmable upstream that, on each subscribe request,
// emits a configured sequence of newHeads notifications and then awaits
// further commands.
type resumeMock struct {
	name      string
	emitFrom  atomic.Int64 // first block to emit on subscribe
	emitCount atomic.Int64 // how many heads to emit before going quiet
	srv       *httptest.Server
	upgrader  websocket.Upgrader
	conns     atomic.Int64
	sentHeads atomic.Int64

	mu      sync.Mutex
	wsConns []*websocket.Conn
}

func newResumeMock(name string, emitFrom, emitCount int64) *resumeMock {
	m := &resumeMock{
		name:     name,
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
	m.emitFrom.Store(emitFrom)
	m.emitCount.Store(emitCount)
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

func (m *resumeMock) URL() string {
	return strings.Replace(m.srv.URL, "http://", "ws://", 1)
}

// Kill force-closes every active WS connection and stops the listener so
// stitch's reconnect logic genuinely fails over to a different backend
// rather than racing with a still-listening primary.
//
// Closing the WS conns first unblocks any in-flight handlers so
// httptest.Server.Close() can drain without deadlocking on our blocking
// reader goroutine.
func (m *resumeMock) Kill() {
	m.mu.Lock()
	conns := m.wsConns
	m.wsConns = nil
	m.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	m.srv.Listener.Close() // refuses new TCP connects; reconnect will ECONNREFUSED
}

func (m *resumeMock) Close() {
	m.Kill()
}

// handle: upgrade, read frames in a goroutine; for any eth_subscribe
// reply with a unique sub id and emit emitCount newHeads at heights
// [emitFrom, emitFrom+emitCount).
func (m *resumeMock) handle(w http.ResponseWriter, r *http.Request) {
	c, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	m.conns.Add(1)
	m.mu.Lock()
	m.wsConns = append(m.wsConns, c)
	m.mu.Unlock()

	subID := fmt.Sprintf("0x%s%s", m.name, time.Now().Format("150405"))

	// Read pump (acknowledge subscribe and ignore the rest).
	subReady := make(chan struct{}, 1)
	subDone := make(chan struct{})
	var once sync.Once
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
			if probe.Method == "eth_subscribe" {
				out, _ := json.Marshal(map[string]any{
					"jsonrpc": "2.0",
					"id":      json.RawMessage(probe.ID),
					"result":  subID,
				})
				_ = c.WriteMessage(websocket.TextMessage, out)
				once.Do(func() { subReady <- struct{}{} })
			}
		}
	}()

	// Wait for the subscribe, then emit heads.
	select {
	case <-subReady:
	case <-time.After(2 * time.Second):
		return
	}

	from := m.emitFrom.Load()
	count := m.emitCount.Load()
	for i := int64(0); i < count; i++ {
		head := fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"%s","result":{"number":"0x%x","hash":"0xhead%d"}}}`, subID, from+i, from+i)
		if err := c.WriteMessage(websocket.TextMessage, []byte(head)); err != nil {
			return
		}
		m.sentHeads.Add(1)
		time.Sleep(5 * time.Millisecond)
	}
	<-subDone
}

func setupResume(t *testing.T, primary, fallback *resumeMock) (*httptest.Server, *health.Registry, func()) {
	t.Helper()
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
	front := httptest.NewServer(srv.Handler())
	return front, h, func() { front.Close() }
}

func TestEthWSResumeAcrossUpstreamFailure(t *testing.T) {
	primary := newResumeMock("primary", 1, 3)   // emits heights 1, 2, 3 then waits
	fallback := newResumeMock("fallback", 3, 3) // emits 3, 4, 5 — 3 should be deduped

	front, h, cleanup := setupResume(t, primary, fallback)
	defer cleanup()
	defer fallback.Close()

	// Connect as a client.
	clientURL := strings.Replace(front.URL, "http://", "ws://", 1)
	d := &websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	c, _, err := d.Dial(clientURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Subscribe to newHeads.
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`)); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Read the subscribe response — record the synthetic id.
	var subResp struct {
		Result string `json:"result"`
	}
	_, msg, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(msg, &subResp); err != nil {
		t.Fatalf("subscribe response: %s (%v)", msg, err)
	}
	syntheticID := subResp.Result
	if syntheticID == "" {
		t.Fatalf("no synthetic id in response: %s", msg)
	}

	// Read heights 1, 2, 3 from primary.
	type head struct {
		Subscription string `json:"subscription"`
		Result       struct {
			Number string `json:"number"`
		} `json:"result"`
	}
	type notif struct {
		Method string `json:"method"`
		Params head   `json:"params"`
	}
	heightsSeen := []int64{}
	subsSeen := map[string]bool{}
	for i := 0; i < 3; i++ {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, msg, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("primary read %d: %v", i, err)
		}
		var n notif
		if err := json.Unmarshal(msg, &n); err != nil {
			t.Fatalf("decode: %v body=%s", err, msg)
		}
		if n.Method != "eth_subscription" {
			t.Fatalf("expected notification, got %s", msg)
		}
		subsSeen[n.Params.Subscription] = true
		var height int64
		fmt.Sscanf(n.Params.Result.Number, "0x%x", &height)
		heightsSeen = append(heightsSeen, height)
	}
	if len(subsSeen) != 1 || !subsSeen[syntheticID] {
		t.Fatalf("synthetic id leaked or absent: seen=%v want=%s", subsSeen, syntheticID)
	}

	// Kill primary mid-stream. Stitch should detect, reconnect to
	// fallback, replay the subscribe, and dedup the duplicate height 3.
	h.Update(health.Snapshot{Backend: "primary", Protocol: types.ProtoRPC, Healthy: false})
	h.Update(health.Snapshot{Backend: "primary", Protocol: types.ProtoEthWS, Healthy: false})
	primary.Kill()

	// Read heights 4 and 5 from fallback (NOT a duplicate of 3).
	for i := 0; i < 2; i++ {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, msg, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("fallback read %d: %v body=%s", i, err, msg)
		}
		var n notif
		if err := json.Unmarshal(msg, &n); err != nil {
			t.Fatalf("decode: %v body=%s", err, msg)
		}
		if n.Params.Subscription != syntheticID {
			t.Errorf("synthetic id changed across resume: got %q want %q", n.Params.Subscription, syntheticID)
		}
		var height int64
		fmt.Sscanf(n.Params.Result.Number, "0x%x", &height)
		heightsSeen = append(heightsSeen, height)
	}

	// Final assertion: heights observed are [1, 2, 3, 4, 5] — strictly
	// monotonic, no duplicates, no gaps.
	want := []int64{1, 2, 3, 4, 5}
	if !equalInt64(heightsSeen, want) {
		t.Errorf("heights = %v; want %v", heightsSeen, want)
	}
	if fallback.conns.Load() != 1 {
		t.Errorf("expected 1 fallback connection (resume); got %d", fallback.conns.Load())
	}
}

func equalInt64(a, b []int64) bool {
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
