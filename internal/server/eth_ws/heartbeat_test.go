package eth_ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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

type heartbeatMessage struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result"`
	Method string          `json:"method"`
	Params struct {
		Subscription string `json:"subscription"`
		Result       struct {
			Number string `json:"number"`
		} `json:"result"`
	} `json:"params"`
}

type heartbeatEvent struct {
	message heartbeatMessage
	err     error
}

// A real transport failure exercises Gorilla's control writer and the session's
// blocked reader, without depending on an OS send buffer filling with tiny Pings.
type heartbeatConn struct {
	net.Conn
	failWrites atomic.Bool
}

func (c *heartbeatConn) Write(p []byte) (int, error) {
	if c.failWrites.Load() {
		return 0, errors.New("injected downstream write failure")
	}
	return c.Conn.Write(p)
}

type heartbeatListener struct {
	net.Listener
	accepted chan *heartbeatConn
}

func (l *heartbeatListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	wrapped := &heartbeatConn{Conn: c}
	l.accepted <- wrapped
	return wrapped, nil
}

type heartbeatRig struct {
	front        *Server
	url          string
	frontDone    chan struct{}
	frontConn    chan *heartbeatConn
	upstreamConn chan *websocket.Conn
	upstreamDone chan struct{}
}

func newHeartbeatRig(t *testing.T, interval time.Duration) *heartbeatRig {
	t.Helper()
	r := &heartbeatRig{
		frontDone: make(chan struct{}), frontConn: make(chan *heartbeatConn, 1),
		upstreamConn: make(chan *websocket.Conn, 1), upstreamDone: make(chan struct{}),
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer close(r.upstreamDone)
		defer c.Close()
		r.upstreamConn <- c
		for {
			var call struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			if err := c.ReadJSON(&call); err != nil {
				return
			}
			var result any = "0x59f"
			var kind string
			switch call.Method {
			case "eth_subscribe":
				if len(call.Params) > 0 {
					_ = json.Unmarshal(call.Params[0], &kind)
				}
				result = "0x" + kind
			case "eth_unsubscribe":
				result = true
			}
			if err := c.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": result}); err != nil {
				return
			}
			if kind == "newHeads" {
				for h := 1; h <= 3; h++ {
					msg := fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xnewHeads","result":{"number":"0x%x","hash":"0xhead%d"}}}`, h, h)
					if err := c.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
						return
					}
				}
			}
		}
	}))
	b := &backend.Backend{
		Name: "heartbeat-upstream", Coverage: backend.Coverage{Kind: backend.CovArchive}, Weight: 100,
		Endpoints: map[types.Protocol]string{types.ProtoEthWS: strings.Replace(upstream.URL, "http://", "ws://", 1)},
	}
	h := health.NewRegistry()
	h.Update(health.Snapshot{Backend: b.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000})
	h.Update(health.Snapshot{Backend: b.Name, Protocol: types.ProtoEthWS, Healthy: true})
	cm := circuit.NewManager(circuit.Policy{ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: time.Second})
	r.front = New("ignored", selector.NewRangeSelector(backend.NewRegistry([]*backend.Backend{b}), h, cm, 0))
	r.front.clientPingInterval = interval
	r.front.clientPingWriteWait = 100 * time.Millisecond
	frontHTTP := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer close(r.frontDone)
		r.front.ServeHTTP(w, req)
	}))
	frontHTTP.Listener = &heartbeatListener{Listener: frontHTTP.Listener, accepted: r.frontConn}
	frontHTTP.Start()
	r.url = strings.Replace(frontHTTP.URL, "http://", "ws://", 1)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := r.front.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
		frontHTTP.Close()
		upstream.Close()
	})
	return r
}

func heartbeatClient(t *testing.T, url string) (*websocket.Conn, <-chan heartbeatEvent, *atomic.Int64) {
	t.Helper()
	dialer := websocket.Dialer{
		HandshakeTimeout: 2 * time.Second,
		NetDialContext:   (&net.Dialer{KeepAlive: -1}).DialContext,
	}
	c, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	pings := &atomic.Int64{}
	respond := c.PingHandler()
	c.SetPingHandler(func(data string) error {
		pings.Add(1)
		return respond(data)
	})
	events := make(chan heartbeatEvent, 256)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			var msg heartbeatMessage
			err := c.ReadJSON(&msg)
			select {
			case events <- heartbeatEvent{message: msg, err: err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		_ = c.Close()
		waitHeartbeatDone(t, done)
	})
	return c, events, pings
}

func nextHeartbeatMessage(t *testing.T, events <-chan heartbeatEvent) heartbeatMessage {
	t.Helper()
	select {
	case ev := <-events:
		if ev.err != nil {
			t.Fatal(ev.err)
		}
		return ev.message
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for WebSocket response")
		return heartbeatMessage{}
	}
}

func heartbeatCall(t *testing.T, c *websocket.Conn, id, method string, params any) {
	t.Helper()
	if err := c.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		t.Fatal(err)
	}
}

func waitHeartbeatDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("WebSocket handler or reader did not stop")
	}
}

type activityWriter struct {
	io.Writer
	activity chan<- struct{}
}

func (w activityWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if n > 0 {
		select {
		case w.activity <- struct{}{}:
		default:
		}
	}
	return n, err
}

// The proxy expires an idle connection based on forwarded bytes, not TCP
// keepalive packets. Control Pings must cross the proxy to keep it alive.
func heartbeatIdleProxy(t *testing.T, target string, idle time.Duration) (string, *atomic.Bool) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	expired := &atomic.Bool{}
	go func() {
		defer close(done)
		client, err := listener.Accept()
		if err != nil {
			return
		}
		defer client.Close()
		upstream, err := net.DialTimeout("tcp", strings.TrimPrefix(target, "ws://"), time.Second)
		if err != nil {
			return
		}
		activity, copied := make(chan struct{}, 1), make(chan struct{}, 2)
		var copies sync.WaitGroup
		copies.Add(2)
		for _, pair := range [][2]net.Conn{{client, upstream}, {upstream, client}} {
			go func(src, dst net.Conn) {
				defer copies.Done()
				_, _ = io.Copy(activityWriter{Writer: dst, activity: activity}, src)
				copied <- struct{}{}
			}(pair[0], pair[1])
		}
		defer func() {
			_ = client.Close()
			_ = upstream.Close()
			copies.Wait()
		}()
		timer := time.NewTimer(idle)
		defer timer.Stop()
		for {
			select {
			case <-stop:
				return
			case <-copied:
				return
			case <-timer.C:
				expired.Store(true)
				return
			case <-activity:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(idle)
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		_ = listener.Close()
		waitHeartbeatDone(t, done)
	})
	return "ws://" + listener.Addr().String(), expired
}

func TestHeartbeatKeepsQuietSubscriptionThroughIdleProxy(t *testing.T) {
	const idle = 300 * time.Millisecond
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("heartbeat=%t", enabled), func(t *testing.T) {
			interval := time.Hour // Negative control: no heartbeat before idle expiry.
			if enabled {
				interval = 25 * time.Millisecond
			}
			r := newHeartbeatRig(t, interval)
			url, expired := heartbeatIdleProxy(t, r.url, idle)
			c, events, pings := heartbeatClient(t, url)
			heartbeatCall(t, c, "quiet", "eth_subscribe", []any{"logs", map[string]any{}})
			ack := nextHeartbeatMessage(t, events)
			var quietID string
			if ack.ID != "quiet" || json.Unmarshal(ack.Result, &quietID) != nil || quietID == "" {
				t.Fatalf("bad quiet subscription response: %+v", ack)
			}
			select {
			case ev := <-events:
				if !enabled && ev.err != nil && expired.Load() && pings.Load() == 0 {
					return
				}
				t.Fatalf("unexpected message or closure while quiet: %+v", ev)
			case <-time.After(3 * idle):
				if !enabled {
					t.Fatal("negative control did not expire")
				}
			}
			if expired.Load() || pings.Load() < 3 {
				t.Fatalf("quiet connection not maintained: expired=%t pings=%d", expired.Load(), pings.Load())
			}
			heartbeatCall(t, c, "unsubscribe", "eth_unsubscribe", []string{quietID})
			heartbeatCall(t, c, "heads", "eth_subscribe", []string{"newHeads"})
			heartbeatCall(t, c, "chain", "eth_chainId", []any{})
			seen := make(map[string]bool)
			var headsID string
			heads := 0
			for len(seen) < 3 || heads < 3 {
				msg := nextHeartbeatMessage(t, events)
				switch msg.ID {
				case "unsubscribe":
					if string(msg.Result) != "true" {
						t.Fatalf("unsubscribe result: %s", msg.Result)
					}
				case "heads":
					if err := json.Unmarshal(msg.Result, &headsID); err != nil || headsID == "" {
						t.Fatalf("newHeads result: %s", msg.Result)
					}
				case "chain":
					if string(msg.Result) != `"0x59f"` {
						t.Fatalf("chain ID result: %s", msg.Result)
					}
				case "":
					heads++
					if msg.Method != "eth_subscription" || msg.Params.Subscription != headsID || msg.Params.Result.Number != fmt.Sprintf("0x%x", heads) {
						t.Fatalf("incorrect subscription notification: %+v", msg)
					}
					continue
				default:
					t.Fatalf("unexpected response ID %q", msg.ID)
				}
				if seen[msg.ID] {
					t.Fatalf("duplicate response ID %q", msg.ID)
				}
				seen[msg.ID] = true
			}
		})
	}
}

func TestHeartbeatConcurrentRPCAndNotifications(t *testing.T) {
	r := newHeartbeatRig(t, 5*time.Millisecond)
	c, events, pings := heartbeatClient(t, r.url)
	heartbeatCall(t, c, "heads", "eth_subscribe", []string{"newHeads"})
	for i := 0; i < 100; i++ {
		heartbeatCall(t, c, fmt.Sprintf("rpc-%03d", i), "eth_chainId", []any{})
		time.Sleep(2 * time.Millisecond)
	}
	seen, heads := make(map[string]bool), 0
	var headsID string
	for len(seen) < 101 || heads < 3 {
		msg := nextHeartbeatMessage(t, events)
		if msg.ID == "" {
			heads++
			if msg.Method != "eth_subscription" || msg.Params.Subscription != headsID || msg.Params.Result.Number != fmt.Sprintf("0x%x", heads) {
				t.Fatalf("incorrect notification: %+v", msg)
			}
			continue
		}
		if msg.ID == "heads" {
			if err := json.Unmarshal(msg.Result, &headsID); err != nil || headsID == "" {
				t.Fatalf("newHeads result: %s", msg.Result)
			}
		} else if string(msg.Result) != `"0x59f"` {
			t.Fatalf("RPC result: %+v", msg)
		}
		if seen[msg.ID] {
			t.Fatalf("duplicate response ID %q", msg.ID)
		}
		seen[msg.ID] = true
	}
	for i := 0; i < 100; i++ {
		if !seen[fmt.Sprintf("rpc-%03d", i)] {
			t.Fatalf("missing original RPC ID %d", i)
		}
	}
	if pings.Load() < 3 {
		t.Fatalf("expected heartbeats during concurrent RPCs; got %d", pings.Load())
	}
}

func TestHeartbeatStopsWithSession(t *testing.T) {
	for _, cause := range []string{"client close", "upstream close", "server shutdown", "ping write failure"} {
		t.Run(cause, func(t *testing.T) {
			r := newHeartbeatRig(t, 25*time.Millisecond)
			c, events, _ := heartbeatClient(t, r.url)
			heartbeatCall(t, c, "chain", "eth_chainId", []any{})
			if msg := nextHeartbeatMessage(t, events); msg.ID != "chain" || string(msg.Result) != `"0x59f"` {
				t.Fatalf("bad initial RPC: %+v", msg)
			}
			switch cause {
			case "client close":
				_ = c.Close()
			case "upstream close":
				_ = (<-r.upstreamConn).Close()
			case "server shutdown":
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := r.front.Shutdown(ctx); err != nil {
					t.Fatal(err)
				}
			case "ping write failure":
				(<-r.frontConn).failWrites.Store(true)
			}
			// ServeHTTP returns only after its Ping loop is joined. The upstream
			// must also close, including when the Ping writer detected failure.
			waitHeartbeatDone(t, r.frontDone)
			waitHeartbeatDone(t, r.upstreamDone)
		})
	}
}
