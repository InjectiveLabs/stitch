package cmt_rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/forwarder"
	"github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
)

type cmtWSMock struct {
	server       *httptest.Server
	requests     chan jsonRPCRequest
	connections  chan *websocket.Conn
	closed       chan struct{}
	pongs        chan string
	httpStarted  chan struct{}
	httpCanceled chan struct{}
	httpCalls    atomic.Int64
	wsCalls      atomic.Int64
	rejectWS     bool
}

func newCMTWSMock(t *testing.T, name string, reject bool) *cmtWSMock {
	t.Helper()
	m := &cmtWSMock{
		requests: make(chan jsonRPCRequest, 32), connections: make(chan *websocket.Conn, 16),
		closed: make(chan struct{}, 16), rejectWS: reject,
		pongs: make(chan string, 16), httpStarted: make(chan struct{}, 1), httpCanceled: make(chan struct{}, 1),
	}
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/websocket" {
			m.httpCalls.Add(1)
			var req jsonRPCRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode upstream HTTP call: %v", err)
				return
			}
			if req.Method == "slow_request" {
				m.httpStarted <- struct{}{}
				<-r.Context().Done()
				m.httpCanceled <- struct{}{}
				return
			}
			if req.Method == "upstream_http_error" {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"backend":%q,"params":%s}}`, req.ID, name, req.Params)
			return
		}
		m.wsCalls.Add(1)
		if m.rejectWS {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		defer func() { m.closed <- struct{}{} }()
		conn.SetPongHandler(func(payload string) error { m.pongs <- payload; return nil })
		m.connections <- conn
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req jsonRPCRequest
			if err := json.Unmarshal(msg, &req); err != nil {
				t.Errorf("invalid upstream subscription request: %v", err)
				return
			}
			m.requests <- req
			response := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{}}`, req.ID))
			if bytes.Contains(req.Params, []byte("reject")) {
				response = []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"error":{"code":-32603,"message":"bad query"}}`, req.ID))
			}
			if err := conn.WriteMessage(websocket.TextMessage, response); err != nil {
				return
			}
			if req.Method == "subscribe" && !bytes.Contains(req.Params, []byte("reject")) {
				event := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"query":"tm.event='NewBlock'","data":{"type":"tendermint/event/NewBlock","value":{"block":{"header":{"height":"201"}}}}}}`, req.ID))
				if err := conn.WriteMessage(websocket.TextMessage, event); err != nil {
					return
				}
			}
		}
	}))
	t.Cleanup(m.server.Close)
	return m
}

type cmtWSRig struct {
	server  *Server
	front   *httptest.Server
	history *cmtWSMock
	tip     *cmtWSMock
}

func newCMTWSRig(t *testing.T, rejectTip bool, extra ...*backend.Backend) *cmtWSRig {
	t.Helper()
	history := newCMTWSMock(t, "history", true)
	tip := newCMTWSMock(t, "tip", rejectTip)
	backends := append([]*backend.Backend{
		{Name: "history", Weight: 100, Coverage: backend.Coverage{Kind: backend.CovBounded, Lower: 1, Upper: 100}, Endpoints: map[types.Protocol]string{types.ProtoRPC: history.server.URL}},
		{Name: "tip", Weight: 200, Coverage: backend.Coverage{Kind: backend.CovPruned, Keep: 10}, Endpoints: map[types.Protocol]string{types.ProtoRPC: tip.server.URL}},
	}, extra...)
	h := health.NewRegistry()
	for _, b := range backends {
		h.Update(health.Snapshot{Backend: b.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 200})
	}
	cm := circuit.NewManager(circuit.Policy{ErrorThreshold: 0.5, MinRequests: 10, OpenDuration: time.Second})
	sel := selector.NewRangeSelector(backend.NewRegistry(backends), h, cm, 0)
	fwd := forwarder.NewHTTP(sel, pool.NewHTTPPool(), cm, forwarder.Policy{MaxAttempts: 3, PerAttemptTimeout: 10 * time.Second})
	s := New("ignored", fwd)
	s.SetWebSocketSelector(sel)
	front := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
		front.Close()
	})
	return &cmtWSRig{server: s, front: front, history: history, tip: tip}
}

func dialCMTWS(t *testing.T, base string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(base, "http")+"/websocket", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func writeCMTWS(t *testing.T, conn *websocket.Conn, msg string) {
	t.Helper()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		t.Fatal(err)
	}
}

func readCMTWS(t *testing.T, conn *websocket.Conn) map[string]json.RawMessage {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(msg, &out); err != nil {
		t.Fatalf("invalid JSON response %s: %v", msg, err)
	}
	return out
}

func assertCMTID(t *testing.T, msg map[string]json.RawMessage, want string) {
	t.Helper()
	if string(msg["id"]) != want {
		t.Fatalf("id = %s, want %s", msg["id"], want)
	}
}

func waitCMTClosed(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream connection did not close")
	}
}

func TestCMTWebSocketSubscriptionsAndHistoricalReads(t *testing.T) {
	for _, id := range []string{`"blocks"`, `9007199254740993`} {
		t.Run(id, func(t *testing.T) {
			rig := newCMTWSRig(t, false)
			conn := dialCMTWS(t, rig.front.URL)
			writeCMTWS(t, conn, fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"method":"subscribe","params":{"query":"tm.event='NewBlock'"}}`, id))
			ack := readCMTWS(t, conn)
			assertCMTID(t, ack, id)
			event := readCMTWS(t, conn)
			assertCMTID(t, event, id)
			if !bytes.Contains(event["result"], []byte(`"height":"201"`)) {
				t.Fatalf("event lost: %s", event["result"])
			}
			for _, request := range []string{
				`{"jsonrpc":"2.0","id":"archive","method":"block","params":{"height":"75"}}`,
				`{"jsonrpc":"2.0","id":"archive","method":"block","params":["75"]}`,
				`{"jsonrpc":"2.0","id":"archive","method":"abci_query","params":["/store/bank/key","ABCD","75",false]}`,
			} {
				writeCMTWS(t, conn, request)
				response := readCMTWS(t, conn)
				assertCMTID(t, response, `"archive"`)
				if !bytes.Contains(response["result"], []byte(`"backend":"history"`)) {
					t.Fatalf("historical read went to tip: %s", response["result"])
				}
			}
			for _, method := range []string{"unsubscribe", "unsubscribe_all"} {
				writeCMTWS(t, conn, fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":%q,"params":{"query":"tm.event='NewBlock'"}}`, method))
				assertCMTID(t, readCMTWS(t, conn), "2")
			}
			for _, method := range []string{"subscribe", "unsubscribe", "unsubscribe_all"} {
				select {
				case req := <-rig.tip.requests:
					if req.Method != method || !bytes.Contains(req.Params, []byte("tm.event='NewBlock'")) {
						t.Fatalf("request changed: %+v", req)
					}
				case <-time.After(time.Second):
					t.Fatal("missing subscription request")
				}
			}
			if rig.tip.httpCalls.Load() != 0 || rig.history.httpCalls.Load() != 3 || rig.tip.wsCalls.Load() != 1 || rig.history.wsCalls.Load() != 0 {
				t.Fatal("unexpected upstream routing")
			}
			_ = conn.Close()
			waitCMTClosed(t, rig.tip.closed)
		})
	}
}

func TestCMTWebSocketBadRequestsAndNotifications(t *testing.T) {
	rig := newCMTWSRig(t, false)
	conn := dialCMTWS(t, rig.front.URL)
	for _, message := range []string{`[`, `[]`, `null`, `{"jsonrpc":"2.0","method":"block","id":{}}`} {
		writeCMTWS(t, conn, message)
		response := readCMTWS(t, conn)
		assertCMTID(t, response, "null")
		if response["error"] == nil {
			t.Fatal("invalid request accepted")
		}
	}
	for _, idField := range []string{"", `,"id":null`} {
		writeCMTWS(t, conn, `{"jsonrpc":"2.0","method":"subscribe","params":{"query":"ignored"}`+idField+`}`)
	}
	writeCMTWS(t, conn, `{"jsonrpc":"2.0","id":3,"method":"block","params":{"height":"75"}}`)
	assertCMTID(t, readCMTWS(t, conn), "3")
	if rig.tip.wsCalls.Load() != 0 || rig.tip.httpCalls.Load() != 0 {
		t.Fatal("invalid frames or notifications reached tip")
	}
}

func TestCMTWebSocketUpstreamErrors(t *testing.T) {
	t.Run("subscribe reject is returned unchanged", func(t *testing.T) {
		rig := newCMTWSRig(t, false)
		conn := dialCMTWS(t, rig.front.URL)
		writeCMTWS(t, conn, `{"jsonrpc":"2.0","id":"bad-query","method":"subscribe","params":{"query":"reject"}}`)
		response := readCMTWS(t, conn)
		assertCMTID(t, response, `"bad-query"`)
		if !bytes.Contains(response["error"], []byte("bad query")) {
			t.Fatal("upstream error lost")
		}
	})
	t.Run("unavailable subscription still permits historical reads", func(t *testing.T) {
		rig := newCMTWSRig(t, true)
		conn := dialCMTWS(t, rig.front.URL)
		writeCMTWS(t, conn, `{"jsonrpc":"2.0","id":42,"method":"subscribe","params":{"query":"tm.event='NewBlock'"}}`)
		response := readCMTWS(t, conn)
		assertCMTID(t, response, "42")
		if response["error"] == nil {
			t.Fatal("missing unavailable error")
		}
		writeCMTWS(t, conn, `{"jsonrpc":"2.0","id":43,"method":"block","params":{"height":"75"}}`)
		assertCMTID(t, readCMTWS(t, conn), "43")
	})
	t.Run("HTTP errors retain request ID", func(t *testing.T) {
		rig := newCMTWSRig(t, false)
		conn := dialCMTWS(t, rig.front.URL)
		writeCMTWS(t, conn, `{"jsonrpc":"2.0","id":"failure","method":"upstream_http_error","params":{}}`)
		response := readCMTWS(t, conn)
		assertCMTID(t, response, `"failure"`)
		if response["error"] == nil {
			t.Fatal("HTTP error did not become JSON-RPC error")
		}
	})
}

func TestCMTWebSocketInitialDialFallback(t *testing.T) {
	fallback := newCMTWSMock(t, "fallback", false)
	rig := newCMTWSRig(t, true, &backend.Backend{
		Name: "fallback", Weight: 100, Coverage: backend.Coverage{Kind: backend.CovPruned, Keep: 10},
		Endpoints: map[types.Protocol]string{types.ProtoRPC: fallback.server.URL},
	})
	conn := dialCMTWS(t, rig.front.URL)
	writeCMTWS(t, conn, `{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"query":"tm.event='NewBlock'"}}`)
	assertCMTID(t, readCMTWS(t, conn), "1")
	assertCMTID(t, readCMTWS(t, conn), "1")
	if rig.tip.wsCalls.Load() != 1 || fallback.wsCalls.Load() != 1 {
		t.Fatal("did not try the next subscription backend")
	}
}

func TestCMTWebSocketUpstreamLossRequiresReconnect(t *testing.T) {
	rig := newCMTWSRig(t, false)
	conn := dialCMTWS(t, rig.front.URL)
	writeCMTWS(t, conn, `{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"query":"tm.event='NewBlock'"}}`)
	readCMTWS(t, conn)
	readCMTWS(t, conn)
	upstream := <-rig.tip.connections
	_ = upstream.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseTryAgainLater) {
		t.Fatalf("expected reconnect close, got %v", err)
	}
}

func TestCMTWebSocketShutdownClosesBothEnds(t *testing.T) {
	rig := newCMTWSRig(t, false)
	conn := dialCMTWS(t, rig.front.URL)
	writeCMTWS(t, conn, `{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"query":"tm.event='NewBlock'"}}`)
	readCMTWS(t, conn)
	readCMTWS(t, conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rig.server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	waitCMTClosed(t, rig.tip.closed)
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("client remains open after shutdown")
	}
}

func TestCMTWebSocketSlowConsumerDisconnects(t *testing.T) {
	serverConn := make(chan *websocket.Conn, 1)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err == nil {
			serverConn <- conn
		}
	}))
	defer front.Close()
	client := dialCMTWS(t, front.URL)
	conn := <-serverConn
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := cmtWSSession{ctx: ctx, cancel: cancel, client: conn, outgoing: make(chan []byte, 1)}
	session.send([]byte(`{"result":1}`))
	session.send([]byte(`{"result":2}`))
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := client.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseTryAgainLater) {
		t.Fatalf("expected explicit slow-consumer close, got %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("slow session was not cancelled")
	}
}

func TestCMTWebSocketControlFramesAndRequestCancellation(t *testing.T) {
	rig := newCMTWSRig(t, false)
	conn := dialCMTWS(t, rig.front.URL)
	writeCMTWS(t, conn, `{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"query":"tm.event='NewBlock'"}}`)
	readCMTWS(t, conn)
	readCMTWS(t, conn)
	upstream := <-rig.tip.connections
	if err := upstream.WriteControl(websocket.PingMessage, []byte("upstream-ping"), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-rig.tip.pongs:
		if payload != "upstream-ping" {
			t.Fatalf("upstream pong changed: %q", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("upstream ping unanswered")
	}

	writeCMTWS(t, conn, `{"jsonrpc":"2.0","id":2,"method":"slow_request","params":{}}`)
	select {
	case <-rig.tip.httpStarted:
	case <-time.After(time.Second):
		t.Fatal("ordinary request did not reach HTTP upstream")
	}
	pongs := make(chan string, 1)
	conn.SetPongHandler(func(payload string) error { pongs <- payload; return nil })
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		_, _, _ = conn.ReadMessage()
	}()
	if err := conn.WriteControl(websocket.PingMessage, []byte("client-ping"), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-pongs:
		if payload != "client-ping" {
			t.Fatalf("client pong changed: %q", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("client ping blocked behind historical request")
	}
	_ = conn.Close()
	waitCMTClosed(t, readerDone)
	waitCMTClosed(t, rig.tip.httpCanceled)
	waitCMTClosed(t, rig.tip.closed)
}

func TestCMTWebSocketRejectsBinaryRequests(t *testing.T) {
	rig := newCMTWSRig(t, false)
	conn := dialCMTWS(t, rig.front.URL)
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"block","params":{}}`)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseUnsupportedData) {
		t.Fatalf("expected unsupported-data close, got %v", err)
	}
}
