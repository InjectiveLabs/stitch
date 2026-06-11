package subscription

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// startInjSession runs an InjSession over a live ws pipe against the
// given mocks and returns the client-side conn plus Run's result channel.
func startInjSession(t *testing.T, cfg InjSessionConfig, tune *connTuning) (*websocket.Conn, chan error) {
	t.Helper()
	srvConn, clientConn := wsPipe(t)
	sess := NewInjSession(srvConn, cfg)
	if tune != nil {
		sess.eng.tuning = *tune
	}
	runDone := make(chan error, 1)
	go func() { runDone <- sess.Run(context.Background()) }()
	return clientConn, runDone
}

// subscribeAndAck issues a subscribe and consumes the "success" ack.
func subscribeAndAck(t *testing.T, c *websocket.Conn) {
	t.Helper()
	req := `{"jsonrpc":"2.0","id":42,"method":"subscribe","params":{"subscription_id":"abc","filter":{}}}`
	if err := c.WriteMessage(websocket.TextMessage, []byte(req)); err != nil {
		t.Fatal(err)
	}
	var ack struct {
		ID     int    `json:"id"`
		Result string `json:"result"`
	}
	readClientJSON(t, c, &ack)
	if ack.Result != "success" || ack.ID != 42 {
		t.Fatalf("subscribe ack: %+v", ack)
	}
}

// readClientJSON reads one frame off the client conn into v.
func readClientJSON(t *testing.T, c *websocket.Conn, v any) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if err := json.Unmarshal(msg, v); err != nil {
		t.Fatalf("decode %s: %v", msg, err)
	}
}

// readClientHeight reads one notification and returns its block_height.
func readClientHeight(t *testing.T, c *websocket.Conn) int64 {
	t.Helper()
	var n struct {
		ID     int `json:"id"`
		Result struct {
			BlockHeight int64 `json:"block_height"`
		} `json:"result"`
	}
	readClientJSON(t, c, &n)
	if n.ID != 42 {
		t.Fatalf("notification id = %d; want 42 (client's original)", n.ID)
	}
	return n.Result.BlockHeight
}

// TestEngineResumeRedialsUntilReplayTimeout: with every upstream dead,
// the engine keeps making dial passes inside the replay window; an
// upstream coming back mid-window resumes the session invisibly.
func TestEngineResumeRedialsUntilReplayTimeout(t *testing.T) {
	mock := newGatedInjMock(t, 1, 2) // first conn emits 1, 2
	sel := newChainStreamSelector(t, mock.HostPort())
	clientConn, runDone := startInjSession(t, InjSessionConfig{
		Selector:      sel,
		ReplayTimeout: 5 * time.Second,
	}, nil)

	subscribeAndAck(t, clientConn)
	if h := readClientHeight(t, clientConn); h != 1 {
		t.Fatalf("height = %d; want 1", h)
	}
	if h := readClientHeight(t, clientConn); h != 2 {
		t.Fatalf("height = %d; want 2", h)
	}

	// Kill the only upstream; gate dials off; the replacement conn will
	// emit 2, 3 (2 must be deduped by the session cursor).
	mock.emitFrom.Store(2)
	mock.accept.Store(false)
	mock.KillConns()

	time.Sleep(600 * time.Millisecond) // a few failed passes
	mock.accept.Store(true)

	if h := readClientHeight(t, clientConn); h != 3 {
		t.Errorf("post-resume height = %d; want 3 (2 deduped)", h)
	}
	select {
	case err := <-runDone:
		t.Fatalf("session terminated during in-window resume: %v", err)
	default:
	}
	if got := mock.conns.Load(); got != 2 {
		t.Errorf("upstream conns = %d; want 2 (one resume)", got)
	}
}

// TestEngineResumeReplayTimeoutElapses: if no upstream becomes dialable
// within the window, the session terminates (and the client conn closes)
// instead of retrying forever.
func TestEngineResumeReplayTimeoutElapses(t *testing.T) {
	mock := newGatedInjMock(t, 1, 1)
	sel := newChainStreamSelector(t, mock.HostPort())
	clientConn, runDone := startInjSession(t, InjSessionConfig{
		Selector:      sel,
		ReplayTimeout: 300 * time.Millisecond,
	}, nil)

	subscribeAndAck(t, clientConn)
	if h := readClientHeight(t, clientConn); h != 1 {
		t.Fatalf("height = %d; want 1", h)
	}

	mock.accept.Store(false)
	mock.KillConns()

	select {
	case err := <-runDone:
		if err == nil {
			t.Error("Run returned nil; want a terminal error after the window elapsed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session did not terminate after the replay window elapsed")
	}
	// The engine closes the client on the way out.
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := clientConn.ReadMessage(); err == nil {
		t.Error("client conn still serving frames after session termination")
	}
}

// TestEngineZeroReplayTimeoutSinglePass guards the pre-knob behavior for
// sessions constructed without ReplayTimeout: a resume gets ONE dial
// pass and the session ends immediately when it fails.
func TestEngineZeroReplayTimeoutSinglePass(t *testing.T) {
	mock := newGatedInjMock(t, 1, 1)
	sel := newChainStreamSelector(t, mock.HostPort())
	clientConn, runDone := startInjSession(t, InjSessionConfig{Selector: sel}, nil)

	subscribeAndAck(t, clientConn)
	if h := readClientHeight(t, clientConn); h != 1 {
		t.Fatalf("height = %d; want 1", h)
	}

	mock.accept.Store(false)
	mock.KillConns()

	start := time.Now()
	select {
	case err := <-runDone:
		if err == nil {
			t.Error("Run returned nil; want a terminal error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("session did not terminate after the single failed pass")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("termination took %v; replay_timeout=0 must not retry", elapsed)
	}
}

// TestEngineQuietUpstreamKeepalive: a quiet-but-healthy upstream must
// outlive the read deadline thanks to keepalive pings. conns staying at
// 1 is the proof — a deadline-killed conn resumes onto a second conn.
func TestEngineQuietUpstreamKeepalive(t *testing.T) {
	mock := newGatedInjMock(t, 9, 1)
	mock.emitDelay.Store(int64(1500 * time.Millisecond)) // ~3× the read deadline below
	sel := newChainStreamSelector(t, mock.HostPort())
	// 500ms deadline / 100ms ping: the wide margin keeps a slow CI's
	// scheduling hiccups from eating the whole pong window.
	tune := connTuning{
		dialTimeout:   2 * time.Second,
		readDeadline:  500 * time.Millisecond,
		pingInterval:  100 * time.Millisecond,
		pingWriteWait: time.Second,
	}
	clientConn, _ := startInjSession(t, InjSessionConfig{Selector: sel}, &tune)

	subscribeAndAck(t, clientConn)
	if h := readClientHeight(t, clientConn); h != 9 {
		t.Errorf("height = %d; want 9", h)
	}
	if got := mock.conns.Load(); got != 1 {
		t.Errorf("upstream conns = %d; want 1 (read deadline must not fire on a quiet stream)", got)
	}
}
