// Package integration — ws_head_routing_test.go exercises the end-to-end
// path from a live WS backend through EthWSProber → health.Registry →
// RangeSelector, locking in the fix for the by-height "no eligible backend"
// race: fake WS backend pushes newHeads, prober updates MaxHead, selector
// returns the backend as eligible at the queried height.
package integration

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

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// wsEthMock emulates an eth_ws backend for integration tests. After
// acknowledging eth_subscribe, the test drives header emission via emit().
// The struct is intentionally self-contained — it deliberately duplicates
// the shape of the package-private wsBackendMock in internal/health so
// this cross-package integration test can stand alone.
type wsEthMock struct {
	srv      *httptest.Server
	upgrader websocket.Upgrader

	mu       sync.Mutex
	conns    []*websocket.Conn
	subAcked atomic.Int64
}

func newWSEthMock(t *testing.T) *wsEthMock {
	t.Helper()
	m := &wsEthMock{
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.close)
	return m
}

// URL returns the HTTP URL of the mock server. The EthWSProber normalises
// http:// → ws:// automatically, so no conversion is needed here.
func (m *wsEthMock) URL() string { return m.srv.URL }

func (m *wsEthMock) handle(w http.ResponseWriter, r *http.Request) {
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
func (m *wsEthMock) emit(t *testing.T, blockNumber int64) {
	t.Helper()
	notif := fmt.Sprintf(
		`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xabc","result":{"number":"0x%x"}}}`,
		blockNumber,
	)
	m.mu.Lock()
	c := m.conns[len(m.conns)-1]
	m.mu.Unlock()
	if err := c.WriteMessage(websocket.TextMessage, []byte(notif)); err != nil {
		t.Fatalf("wsEthMock.emit: %v", err)
	}
}

func (m *wsEthMock) close() {
	m.mu.Lock()
	conns := m.conns
	m.conns = nil
	m.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	m.srv.Close()
}

// waitFor polls cond() until it returns true or the deadline expires.
func waitFor(t *testing.T, deadline time.Duration, label string, cond func() bool) {
	t.Helper()
	timer := time.After(deadline)
	for {
		if cond() {
			return
		}
		select {
		case <-timer:
			t.Fatalf("timed out waiting for: %s", label)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestWSHeadRoutingIntegration verifies the full loop:
//
//	fake WS backend → EthWSProber pushes newHeads → MaxHead advances →
//	RangeSelector returns the backend as eligible at the queried height.
//
// This locks in the fix for the by-height "no eligible backend" race: without
// the MaxHead floor, a ClassByHeight query at block N would find zero
// candidates because MaxHead had not yet advanced to at least N.
func TestWSHeadRoutingIntegration(t *testing.T) {
	mock := newWSEthMock(t)

	const backendName = "eth-tip"
	const block1 int64 = 168_438_923
	const block2 int64 = block1 + 10

	// Build backend registry. The backend advertises both eth_rpc (for
	// selector routing) and eth_ws (for the EthWSProber). Coverage is
	// CovArchive so Eligible(N, head) is true for any N ≤ head.
	bs := []*backend.Backend{{
		Name:     backendName,
		Coverage: backend.Coverage{Kind: backend.CovArchive},
		Weight:   100,
		Endpoints: map[types.Protocol]string{
			types.ProtoEthRPC: "http://eth-tip:8545", // not dialed in this test
			types.ProtoEthWS:  mock.URL(),
		},
	}}
	reg := backend.NewRegistry(bs)

	h := health.NewRegistry()

	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    4,
		OpenDuration:   100 * time.Millisecond,
	})

	sel := selector.NewRangeSelector(reg, h, cm, 0)

	// Wire up the prober. The timing knobs (refresh, baseBackoff,
	// healthyStreamThreshold) are unexported; we don't need to override them
	// here because the prober reconciles immediately on Run() and the mock
	// backend connects on the first attempt — no reconnect or refresh cycle
	// is exercised. The test polls with a 2s deadline, well within the
	// default 30s refresh tick.
	p := health.NewEthWSProber(reg, h)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	// --- Phase 1: first header ------------------------------------------------

	// Wait until the prober has subscribed (ack received).
	waitFor(t, 2*time.Second, "prober subscribe ack", func() bool {
		return mock.subAcked.Load() >= 1
	})

	// Drive the mock to emit block1 and wait for MaxHead to reflect it.
	mock.emit(t, block1)
	waitFor(t, 2*time.Second, fmt.Sprintf("MaxHead >= %d", block1), func() bool {
		return h.MaxHead() >= block1
	})

	// Ask the selector for candidates at block1 via ClassByHeight.
	height1 := block1
	key1 := types.RouteKey{
		Protocol: types.ProtoEthRPC,
		Method:   "eth_getBalance",
		Class:    types.ClassByHeight,
		Height:   &height1,
	}
	candidates := sel.Candidates(key1)
	if len(candidates) == 0 {
		t.Fatalf("phase 1: Candidates returned empty list at height %d (MaxHead=%d)",
			block1, h.MaxHead())
	}
	found := false
	for _, c := range candidates {
		if c.Name == backendName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("phase 1: backend %q not in candidates at height %d; got %v",
			backendName, block1, candidateNames(candidates))
	}

	// --- Phase 2: second header (proves the live update loop) -----------------

	mock.emit(t, block2)
	waitFor(t, 2*time.Second, fmt.Sprintf("MaxHead >= %d", block2), func() bool {
		return h.MaxHead() >= block2
	})

	height2 := block2
	key2 := types.RouteKey{
		Protocol: types.ProtoEthRPC,
		Method:   "eth_getBalance",
		Class:    types.ClassByHeight,
		Height:   &height2,
	}
	candidates2 := sel.Candidates(key2)
	if len(candidates2) == 0 {
		t.Fatalf("phase 2: Candidates returned empty list at height %d (MaxHead=%d)",
			block2, h.MaxHead())
	}
	found2 := false
	for _, c := range candidates2 {
		if c.Name == backendName {
			found2 = true
			break
		}
	}
	if !found2 {
		t.Errorf("phase 2: backend %q not in candidates at height %d; got %v",
			backendName, block2, candidateNames(candidates2))
	}
}

func candidateNames(bs []*backend.Backend) []string {
	names := make([]string, len(bs))
	for i, b := range bs {
		names[i] = b.Name
	}
	return names
}
