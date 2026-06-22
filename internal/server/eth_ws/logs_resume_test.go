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

// logsMock emits eth_subscription notifications for the `logs` kind, with
// (blockNumber, transactionIndex, logIndex) fields the cursor machinery
// uses for dedup.
type logsMock struct {
	name     string
	emitFrom atomic.Int64 // first block number
	emitN    atomic.Int64
	srv      *httptest.Server
	upgrader websocket.Upgrader

	mu      sync.Mutex
	wsConns []*websocket.Conn
}

func newLogsMock(name string, emitFrom, emitN int64) *logsMock {
	m := &logsMock{
		name:     name,
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
	m.emitFrom.Store(emitFrom)
	m.emitN.Store(emitN)
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

func (m *logsMock) URL() string {
	return strings.Replace(m.srv.URL, "http://", "ws://", 1)
}

func (m *logsMock) Kill() {
	m.mu.Lock()
	conns := m.wsConns
	m.wsConns = nil
	m.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	m.srv.Listener.Close()
}

func (m *logsMock) handle(w http.ResponseWriter, r *http.Request) {
	c, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	m.mu.Lock()
	m.wsConns = append(m.wsConns, c)
	m.mu.Unlock()

	subID := fmt.Sprintf("0x%s%d", m.name, time.Now().UnixNano())
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
				ack, _ := json.Marshal(map[string]any{
					"jsonrpc": "2.0",
					"id":      json.RawMessage(probe.ID),
					"result":  subID,
				})
				_ = c.WriteMessage(websocket.TextMessage, ack)
				once.Do(func() { subReady <- struct{}{} })
			}
		}
	}()

	select {
	case <-subReady:
	case <-time.After(2 * time.Second):
		return
	}

	from := m.emitFrom.Load()
	count := m.emitN.Load()
	for i := int64(0); i < count; i++ {
		notif := fmt.Sprintf(
			`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"%s","result":{"blockNumber":"0x%x","transactionIndex":"0x0","logIndex":"0x%x","data":"0xabcd"}}}`,
			subID, from+i, i,
		)
		if err := c.WriteMessage(websocket.TextMessage, []byte(notif)); err != nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	<-subDone
}

type cursorPair struct{ block, logIdx uint64 }

func setupLogsRig(t *testing.T, primary, fallback *logsMock) (*httptest.Server, *health.Registry, func()) {
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
		h.Update(health.Snapshot{Backend: bb.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000})
		h.Update(health.Snapshot{Backend: bb.Name, Protocol: types.ProtoEthWS, Healthy: true})
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: 100 * time.Millisecond,
	})
	sel := selector.NewRangeSelector(reg, h, cm, 0)
	srv := New("ignored", sel)
	front := httptest.NewServer(srv.Handler())
	return front, h, func() { front.Close() }
}

func TestEthWSLogsResumeAcrossUpstreamFailure(t *testing.T) {
	primary := newLogsMock("primary", 1, 3)   // emits blockNumber=1,2,3 logIndex=0,1,2
	fallback := newLogsMock("fallback", 3, 3) // emits blockNumber=3,4,5 logIndex=0,1,2 — bn=3 deduped
	front, h, cleanup := setupLogsRig(t, primary, fallback)
	defer cleanup()
	defer fallback.Kill()

	d := &websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	clientURL := strings.Replace(front.URL, "http://", "ws://", 1)
	c, _, err := d.Dial(clientURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Subscribe to logs.
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["logs",{}]}`)); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

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
		t.Fatalf("no synthetic id: %s", msg)
	}

	type logEvent struct {
		Method string `json:"method"`
		Params struct {
			Subscription string `json:"subscription"`
			Result       struct {
				BlockNumber      string `json:"blockNumber"`
				TransactionIndex string `json:"transactionIndex"`
				LogIndex         string `json:"logIndex"`
			} `json:"result"`
		} `json:"params"`
	}

	got := []cursorPair{}
	readN := func(n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, msg, err := c.ReadMessage()
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			var ev logEvent
			if err := json.Unmarshal(msg, &ev); err != nil {
				t.Fatalf("decode: %v body=%s", err, msg)
			}
			if ev.Params.Subscription != syntheticID {
				t.Errorf("synthetic id changed: got %s want %s", ev.Params.Subscription, syntheticID)
			}
			var bn, li uint64
			fmt.Sscanf(ev.Params.Result.BlockNumber, "0x%x", &bn)
			fmt.Sscanf(ev.Params.Result.LogIndex, "0x%x", &li)
			got = append(got, cursorPair{block: bn, logIdx: li})
		}
	}

	// Drain primary's 3 logs at block 1, 2, 3 (logIndex 0, 1, 2 respectively).
	readN(3)

	h.Update(health.Snapshot{Backend: "primary", Protocol: types.ProtoRPC, Healthy: false})
	h.Update(health.Snapshot{Backend: "primary", Protocol: types.ProtoEthWS, Healthy: false})
	primary.Kill()

	// Fallback emits blockNumber 3 (logIndex 0), 4 (logIndex 1), 5 (logIndex 2).
	// The first one shares (block=3, logIndex=0) but the cursor includes
	// logIndex; primary's last delivered cursor was (block=3, logIndex=2).
	// fallback's first event (block=3, logIndex=0) is strictly behind that
	// — it should be deduped. (block=3 logIndex=2 == cursor — also dropped.)
	// Actually wait, blockNumber=3,logIndex=0 < (3,2), so dedup yes.
	// Then (4, 1) advances cursor; (5, 2) advances further.
	readN(2)

	want := []cursorPair{
		{1, 0}, {2, 1}, {3, 2}, // primary
		{4, 1}, {5, 2}, // fallback (3,0) and (3,1) deduped
	}
	if !equalCursorPairs(got, want) {
		t.Errorf("got %v; want %v", got, want)
	}
}

func equalCursorPairs(a, b []cursorPair) bool {
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
