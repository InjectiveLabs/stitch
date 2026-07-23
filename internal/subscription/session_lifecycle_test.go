package subscription

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/metrics"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// wsPipe returns the two ends of a live WebSocket connection: the
// server-side conn (what a session treats as its client) and the
// client-side conn the test writes into.
func wsPipe(t *testing.T) (server, client *websocket.Conn) {
	t.Helper()
	connCh := make(chan *websocket.Conn, 1)
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connCh <- c
	}))
	t.Cleanup(hs.Close)

	d := &websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	cl, _, err := d.Dial(strings.Replace(hs.URL, "http://", "ws://", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	select {
	case server = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("wsPipe: server conn never arrived")
	}
	t.Cleanup(func() { _ = server.Close() })
	return server, cl
}

// gatedSelector blocks Candidates until gate closes, then reports no
// candidates. It parks Run inside dialUpstream so a test can drive the
// clientReader into a known state before Run unwinds.
type gatedSelector struct {
	gate <-chan struct{}
}

func (g gatedSelector) Candidates(types.RouteKey) []*backend.Backend {
	<-g.gate
	return nil
}

// goroutineBlockedInSend reports whether any goroutine is parked on its
// clientCh send inside the named function. The park state is "chan send"
// for a bare send, "select" when the send is raced against done; a reader
// parked in ReadMessage shows "IO wait" and matches neither.
func goroutineBlockedInSend(fn string) bool {
	s := allStacks()
	for _, g := range strings.Split(s, "\n\n") {
		if strings.Contains(g, fn) &&
			(strings.Contains(g, "[chan send") || strings.Contains(g, "[select")) {
			return true
		}
	}
	return false
}

// goroutineExists reports whether any goroutine stack mentions fn.
func goroutineExists(fn string) bool {
	return strings.Contains(allStacks(), fn)
}

// allStacks returns the full goroutine dump, growing the buffer until
// runtime.Stack fills less than the allocated capacity (max 64 MiB).
func allStacks() string {
	const maxBuf = 64 << 20
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) || len(buf) >= maxBuf {
			return string(buf[:n])
		}
		buf = make([]byte, len(buf)*2)
	}
}

// pollUntil retries cond every 10ms until it holds or the deadline expires.
func pollUntil(t *testing.T, deadline time.Duration, label string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for {
		if cond() {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for: %s", label)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSessionClientReaderExitsAfterRunReturns locks in the teardown
// contract: once Run returns, the clientReader goroutine must exit even
// if it is blocked sending into a full clientCh — closing the client
// conn only unblocks ReadMessage, not a parked channel send.
func TestSessionClientReaderExitsAfterRunReturns(t *testing.T) {
	srvConn, clientConn := wsPipe(t)

	gate := make(chan struct{})
	sess := NewSession(srvConn, SessionConfig{Selector: gatedSelector{gate: gate}})

	runDone := make(chan error, 1)
	go func() { runDone <- sess.Run(context.Background()) }()

	// Stuff well past clientCh's capacity (32) while Run is parked in the
	// selector: the reader must end up blocked on the channel send.
	for i := 0; i < 40; i++ {
		if err := clientConn.WriteMessage(websocket.TextMessage, []byte(`{"id":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	pollUntil(t, 3*time.Second, "clientReader parked in chan send", func() bool {
		return goroutineBlockedInSend("(*Session).clientReader")
	})

	close(gate) // selector reports no candidates → Run returns

	select {
	case <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return")
	}

	pollUntil(t, 3*time.Second, "clientReader goroutine exit", func() bool {
		return !goroutineExists("(*Session).clientReader")
	})
}

// TestInjSessionClientReaderExitsAfterRunReturns is the InjSession twin
// of the test above.
func TestInjSessionClientReaderExitsAfterRunReturns(t *testing.T) {
	srvConn, clientConn := wsPipe(t)

	gate := make(chan struct{})
	sess := NewInjSession(srvConn, InjSessionConfig{Selector: gatedSelector{gate: gate}})

	runDone := make(chan error, 1)
	go func() { runDone <- sess.Run(context.Background()) }()

	for i := 0; i < 40; i++ {
		if err := clientConn.WriteMessage(websocket.TextMessage, []byte(`{"id":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	pollUntil(t, 3*time.Second, "clientReader parked in chan send", func() bool {
		return goroutineBlockedInSend("(*InjSession).clientReader")
	})

	close(gate)

	select {
	case <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return")
	}

	pollUntil(t, 3*time.Second, "clientReader goroutine exit", func() bool {
		return !goroutineExists("(*InjSession).clientReader")
	})
}

// TestSessionUnknownSubNotificationDropIsCounted asserts the silent drop
// of a notification for an unknown upstream subscription increments the
// dropped-notifications counter.
func TestSessionUnknownSubNotificationDropIsCounted(t *testing.T) {
	srvConn, _ := wsPipe(t)
	sess := NewSession(srvConn, SessionConfig{})

	lbl := metrics.SubscriptionDroppedNotifs.WithLabelValues(string(types.ProtoEthWS), "unknown_sub")
	before := testutil.ToFloat64(lbl)

	notif := []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xdead","result":{"number":"0x5"}}}`)
	if err := sess.routeUpstreamFrame(context.Background(), notif); err != nil {
		t.Fatalf("routeUpstreamFrame: %v", err)
	}

	if got := testutil.ToFloat64(lbl) - before; got != 1 {
		t.Errorf("dropped-notifications delta = %v; want 1", got)
	}
}

// TestInjSessionUnknownSubNotificationDropIsCounted is the InjSession
// twin: a notification whose id matches no live or pending sub.
func TestInjSessionUnknownSubNotificationDropIsCounted(t *testing.T) {
	srvConn, _ := wsPipe(t)
	sess := NewInjSession(srvConn, InjSessionConfig{})

	lbl := metrics.SubscriptionDroppedNotifs.WithLabelValues(string(types.ProtoChainStream), "unknown_sub")
	before := testutil.ToFloat64(lbl)

	notif := []byte(`{"jsonrpc":"2.0","id":"stitch_inj_99","result":{"block_height":7,"block_time":1}}`)
	if err := sess.routeUpstreamFrame(context.Background(), notif); err != nil {
		t.Fatalf("routeUpstreamFrame: %v", err)
	}

	if got := testutil.ToFloat64(lbl) - before; got != 1 {
		t.Errorf("dropped-notifications delta = %v; want 1", got)
	}
}
