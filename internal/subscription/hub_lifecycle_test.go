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
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/health"
	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/types"
)

// gatedInjMock is a programmable /injstream-ws upstream for lifecycle
// tests: upgrades can be gated off (HTTP 503 → dial failure), emission is
// configurable per connection (start height, count, delay after the
// subscribe ack), and live conns can be killed to force a resume. The
// handler's reader goroutine keeps reading throughout, so gorilla's
// default ping handler answers keepalive pings.
type gatedInjMock struct {
	srv      *httptest.Server
	upgrader websocket.Upgrader

	accept          atomic.Bool
	rejectSubscribe atomic.Bool // answer subscribe with a JSON-RPC error instead of the ack
	conns           atomic.Int64
	emitFrom        atomic.Int64
	emitN           atomic.Int64
	emitDelay       atomic.Int64 // nanoseconds between subscribe ack and first emit

	mu      sync.Mutex
	wsConns []*websocket.Conn
}

func newGatedInjMock(t *testing.T, emitFrom, emitN int64) *gatedInjMock {
	t.Helper()
	m := &gatedInjMock{
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
	m.accept.Store(true)
	m.emitFrom.Store(emitFrom)
	m.emitN.Store(emitN)
	mux := http.NewServeMux()
	mux.HandleFunc("/injstream-ws", m.handle)
	m.srv = httptest.NewServer(mux)
	t.Cleanup(func() {
		m.KillConns()
		m.srv.Close()
	})
	return m
}

func (m *gatedInjMock) HostPort() string {
	return strings.TrimPrefix(m.srv.URL, "http://")
}

// KillConns closes every live upstream-side conn without stopping the
// listener — the next dial succeeds iff accept is still true.
func (m *gatedInjMock) KillConns() {
	m.mu.Lock()
	conns := m.wsConns
	m.wsConns = nil
	m.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (m *gatedInjMock) handle(w http.ResponseWriter, r *http.Request) {
	if !m.accept.Load() {
		http.Error(w, "gated", http.StatusServiceUnavailable)
		return
	}
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
	// Single reader for the conn's whole life: acks subscribes and — by
	// virtue of reading — lets gorilla answer pings with pongs.
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
				if m.rejectSubscribe.Load() {
					reject, _ := json.Marshal(map[string]any{
						"jsonrpc": "2.0",
						"id":      json.RawMessage(probe.ID),
						"error":   map[string]any{"code": -32602, "message": "filter rejected"},
					})
					_ = writeFrame(reject)
					continue
				}
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

	if d := time.Duration(m.emitDelay.Load()); d > 0 {
		select {
		case <-time.After(d):
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

// newGatedHub builds a Hub whose selector routes chainstream to the mocks.
func newGatedHub(t *testing.T, mocks ...*gatedInjMock) *Hub {
	t.Helper()
	hostports := make([]string, len(mocks))
	for i, m := range mocks {
		hostports[i] = m.HostPort()
	}
	return NewHub(newChainStreamSelector(t, hostports...), &websocket.Dialer{HandshakeTimeout: 2 * time.Second})
}

// newChainStreamSelector wires a RangeSelector over healthy archive
// backends whose chainstream endpoint is each given host:port. Weights
// descend with position so candidate order is deterministic.
func newChainStreamSelector(t *testing.T, hostports ...string) selector.Selector {
	t.Helper()
	bs := make([]*backend.Backend, len(hostports))
	for i, hp := range hostports {
		bs[i] = &backend.Backend{
			Name:      fmt.Sprintf("gated-u%d", i),
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    200 - i,
			Endpoints: map[types.Protocol]string{types.ProtoChainStream: hp},
		}
	}
	reg := backend.NewRegistry(bs)
	h := health.NewRegistry()
	for _, b := range bs {
		h.Update(health.Snapshot{Backend: b.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000})
		h.Update(health.Snapshot{Backend: b.Name, Protocol: types.ProtoChainStream, Healthy: true})
	}
	cm := circuit.NewManager(circuit.Policy{ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: 100 * time.Millisecond})
	return selector.NewRangeSelector(reg, h, cm, 0)
}

// readHeight pulls the next fan-out frame and extracts block_height.
func readHeight(t *testing.T, sub *Subscriber, deadline time.Duration) int64 {
	t.Helper()
	select {
	case msg := <-sub.Out:
		var env struct {
			Result struct {
				BlockHeight int64 `json:"block_height"`
			} `json:"result"`
		}
		if err := json.Unmarshal(msg, &env); err != nil {
			t.Fatalf("decode notification %s: %v", msg, err)
		}
		return env.Result.BlockHeight
	case <-sub.Done():
		t.Fatal("subscriber terminated while awaiting a notification")
	case <-time.After(deadline):
		t.Fatal("timed out awaiting a notification")
	}
	return 0
}

// TestHubZeroClientUpstreamExitsPromptly locks in the detach fix: with a
// QUIET upstream (no frames after the subscribe ack) the run loop is
// parked in ReadMessage, and the last detach must interrupt that read —
// the old detachCh logic only noticed emptiness when a frame happened to
// arrive, leaving the conn parked until the 60s read deadline.
func TestHubZeroClientUpstreamExitsPromptly(t *testing.T) {
	mock := newGatedInjMock(t, 1, 0) // acks subscribe, emits nothing
	hub := newGatedHub(t, mock)

	sub, err := hub.Subscribe(context.Background(), json.RawMessage(`{"quiet":1}`), "c1", json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	// Wait until the upstream is actually connected and parked reading.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && mock.conns.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if mock.conns.Load() != 1 {
		t.Fatalf("upstream never connected; conns=%d", mock.conns.Load())
	}

	sub.Detach()

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.UpstreamCount() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("zero-client upstream still alive after detach; UpstreamCount=%d", hub.UpstreamCount())
}

// TestHubShutdownStopsUpstreams: Shutdown must tear down upstream
// goroutines blocked in ReadMessage, fire every subscriber's Done, and
// refuse later Subscribes.
func TestHubShutdownStopsUpstreams(t *testing.T) {
	mock := newGatedInjMock(t, 1, 0) // quiet: the worst case for teardown
	hub := newGatedHub(t, mock)

	sub, err := hub.Subscribe(context.Background(), json.RawMessage(`{"s":1}`), "c1", json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && mock.conns.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := hub.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-sub.Done():
	default:
		t.Error("subscriber Done did not fire on hub shutdown")
	}
	if n := hub.UpstreamCount(); n != 0 {
		t.Errorf("UpstreamCount after shutdown: %d", n)
	}
	if _, err := hub.Subscribe(context.Background(), json.RawMessage(`{"s":2}`), "c2", json.RawMessage(`2`)); err == nil {
		t.Error("Subscribe after Shutdown should fail")
	}
}

// TestHubSlowConsumerDropCountsMetric: the drop policy's evictions must
// be visible in SubscriptionDroppedNotifs{chainstream,slow_consumer},
// not only in the test-facing per-subscriber DropCount.
func TestHubSlowConsumerDropCountsMetric(t *testing.T) {
	mock := newGatedInjMock(t, 1, 100)
	hub := newGatedHub(t, mock)
	hub.SlowConsumer = "drop"
	hub.SendBufSize = 2

	lbl := metrics.SubscriptionDroppedNotifs.WithLabelValues(string(types.ProtoChainStream), "slow_consumer")
	before := testutil.ToFloat64(lbl)

	slow, err := hub.Subscribe(context.Background(), json.RawMessage(`{"m":1}`), "slow", json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Detach()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && slow.DropCount() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if slow.DropCount() == 0 {
		t.Fatal("slow consumer accumulated no drops")
	}
	slow.Detach()
	drops := slow.DropCount()
	var got float64
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got = testutil.ToFloat64(lbl) - before
		if got >= float64(drops) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got < float64(drops) {
		t.Errorf("slow_consumer metric delta = %v; want ≥ DropCount %d", got, drops)
	}
}

// TestHubSlowConsumerDisconnectDetachesAndCounts: the disconnect policy
// must fire Done on the laggard and count the lost notification.
func TestHubSlowConsumerDisconnectDetachesAndCounts(t *testing.T) {
	mock := newGatedInjMock(t, 1, 100)
	hub := newGatedHub(t, mock)
	hub.SlowConsumer = "disconnect"
	hub.SendBufSize = 1

	lbl := metrics.SubscriptionDroppedNotifs.WithLabelValues(string(types.ProtoChainStream), "slow_consumer")
	before := testutil.ToFloat64(lbl)

	slow, err := hub.Subscribe(context.Background(), json.RawMessage(`{"d":1}`), "slow", json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-slow.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("disconnect policy never detached the slow consumer")
	}
	if got := testutil.ToFloat64(lbl) - before; got < 1 {
		t.Errorf("slow_consumer metric delta = %v; want ≥ 1", got)
	}
}

// TestHubQuietUpstreamKeepalive: a quiet-but-healthy upstream must stay
// connected past the read deadline — the keepalive pings refresh it via
// pongs. conns staying at 1 is the proof: without pings the deadline
// kills the conn and the resume re-dial bumps it to 2.
func TestHubQuietUpstreamKeepalive(t *testing.T) {
	mock := newGatedInjMock(t, 7, 1)
	mock.emitDelay.Store(int64(1500 * time.Millisecond)) // ~3× the read deadline below
	hub := newGatedHub(t, mock)
	// 500ms deadline / 100ms ping: the wide margin keeps a slow CI's
	// scheduling hiccups from eating the whole pong window.
	hub.tuning = connTuning{
		dialTimeout:   2 * time.Second,
		readDeadline:  500 * time.Millisecond,
		pingInterval:  100 * time.Millisecond,
		pingWriteWait: time.Second,
	}

	sub, err := hub.Subscribe(context.Background(), json.RawMessage(`{"k":1}`), "c1", json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Detach()

	if h := readHeight(t, sub, 5*time.Second); h != 7 {
		t.Errorf("height = %d; want 7", h)
	}
	if got := mock.conns.Load(); got != 1 {
		t.Errorf("upstream conns = %d; want 1 (read deadline must not fire on a quiet stream)", got)
	}
}

// TestHubResumeRedialWithinWindow: after upstream death, the hub keeps
// re-dialing inside the ReplayTimeout window; once a backend answers it
// replays the subscribe and the shared cursor dedups the overlap.
func TestHubResumeRedialWithinWindow(t *testing.T) {
	mock := newGatedInjMock(t, 1, 2) // first conn emits 1, 2
	hub := newGatedHub(t, mock)
	hub.ReplayTimeout = 5 * time.Second

	sub, err := hub.Subscribe(context.Background(), json.RawMessage(`{"r":1}`), "c1", json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Detach()

	if h := readHeight(t, sub, 3*time.Second); h != 1 {
		t.Fatalf("first height = %d; want 1", h)
	}
	if h := readHeight(t, sub, 3*time.Second); h != 2 {
		t.Fatalf("second height = %d; want 2", h)
	}

	// Kill the upstream and gate dials off; the next conn would emit 2, 3
	// (2 must be deduped by the shared cursor).
	mock.emitFrom.Store(2)
	mock.accept.Store(false)
	mock.KillConns()

	// Reopen the gate mid-window.
	time.Sleep(600 * time.Millisecond)
	mock.accept.Store(true)

	if h := readHeight(t, sub, 5*time.Second); h != 3 {
		t.Errorf("post-resume height = %d; want 3 (2 deduped, no duplicates)", h)
	}
	if got := mock.conns.Load(); got != 2 {
		t.Errorf("upstream conns = %d; want 2 (one resume)", got)
	}
}

// TestHubUpstreamSubscribeRejectEndsUpstream: an upstream that answers
// the hub's subscribe with a JSON-RPC error must END the shared upstream
// rather than leave a silently dead subscription behind the synthesized
// attach-time acks — every attached subscriber's Done fires (so sessions
// close their clients with 1013), the reject is counted under
// SubscriptionDroppedNotifs{upstream_reject}, the upstream leaves the hub,
// and its run goroutine exits. It must NOT resume: replaying the same
// rejected filter would be rejected again, forever.
func TestHubUpstreamSubscribeRejectEndsUpstream(t *testing.T) {
	mock := newGatedInjMock(t, 1, 0)
	mock.rejectSubscribe.Store(true)
	hub := newGatedHub(t, mock)
	hub.ReplayTimeout = 5 * time.Second // must NOT be consumed: reject ≠ resume

	lbl := metrics.SubscriptionDroppedNotifs.WithLabelValues(string(types.ProtoChainStream), "upstream_reject")
	before := testutil.ToFloat64(lbl)

	a, err := hub.Subscribe(context.Background(), json.RawMessage(`{"rej":1}`), "ca", json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := hub.Subscribe(context.Background(), json.RawMessage(`{"rej":1}`), "cb", json.RawMessage(`2`))
	if err != nil {
		t.Fatal(err)
	}

	for name, sub := range map[string]*Subscriber{"a": a, "b": b} {
		select {
		case <-sub.Done():
		case <-time.After(3 * time.Second):
			t.Fatalf("subscriber %s not detached after upstream subscribe reject", name)
		}
	}
	if got := testutil.ToFloat64(lbl) - before; got < 1 {
		t.Errorf("upstream_reject metric delta = %v; want ≥ 1", got)
	}
	pollUntil(t, 2*time.Second, "upstream removed from hub", func() bool {
		return hub.UpstreamCount() == 0
	})
	pollUntil(t, 2*time.Second, "hub upstream run goroutine exit", func() bool {
		return !goroutineExists("hubUpstream).run")
	})
	// No resume: a redial would land within resumeBackoffMin (250ms); give
	// it room and require the conn count to stay put.
	conns := mock.conns.Load()
	time.Sleep(400 * time.Millisecond)
	if got := mock.conns.Load(); got != conns {
		t.Errorf("upstream conns grew %d → %d after reject; a rejected subscribe must not be replayed", conns, got)
	}
}

// TestHubResumeWindowElapses: when no backend becomes dialable within
// ReplayTimeout, the upstream gives up and detaches its subscribers.
func TestHubResumeWindowElapses(t *testing.T) {
	mock := newGatedInjMock(t, 1, 1)
	hub := newGatedHub(t, mock)
	hub.ReplayTimeout = 300 * time.Millisecond

	sub, err := hub.Subscribe(context.Background(), json.RawMessage(`{"w":1}`), "c1", json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	if h := readHeight(t, sub, 3*time.Second); h != 1 {
		t.Fatalf("height = %d; want 1", h)
	}

	mock.accept.Store(false)
	mock.KillConns()

	select {
	case <-sub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber not detached after the replay window elapsed")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.UpstreamCount() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("upstream entry still present after giving up; UpstreamCount=%d", hub.UpstreamCount())
}
