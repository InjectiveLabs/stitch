package cmt_rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/InjectiveLabs/stitch/internal/cache"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
	"github.com/InjectiveLabs/stitch/internal/wsurl"
)

const (
	wsReadLimit  = 32 << 20
	wsQueueSize  = 16
	wsQueueBytes = 2 * wsReadLimit
	wsWriteWait  = 10 * time.Second
	wsReadWait   = 60 * time.Second
	wsPingEvery  = 20 * time.Second
)

// SetWebSocketSelector enables subscription routing. Call before Start.
// Ordinary WebSocket calls use the HTTP forwarder, including its height
// selection, retries and cache. Subscription methods share a dedicated tip
// connection per client. CometBFT subscriptions cannot replay missed events,
// so upstream loss closes the client with 1013 instead of hiding gaps during
// a reconnect. Clients must reconnect and reconcile missed events themselves.
func (s *Server) SetWebSocketSelector(sel selector.Selector) { s.wsSelector = sel }

func (s *Server) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		ReadBufferSize: 4096, WriteBufferSize: 4096,
		// Match CometBFT: authentication and origin policy belong at ingress.
		CheckOrigin: func(*http.Request) bool { return true },
	}
	client, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	if !s.wsTracker.Track(client) {
		_ = client.Close()
		return
	}
	defer s.wsTracker.Untrack(client)

	ctx, cancel := context.WithCancel(r.Context())
	session := &cmtWSSession{
		server: s, client: client, ctx: ctx, cancel: cancel,
		incoming: make(chan []byte, wsQueueSize), outgoing: make(chan []byte, wsQueueSize),
	}
	session.run()
}

type cmtWSSession struct {
	server        *Server
	client        *websocket.Conn
	ctx           context.Context
	cancel        context.CancelFunc
	incoming      chan []byte
	outgoing      chan []byte
	workers       sync.WaitGroup
	incomingBytes atomic.Int64
	outgoingBytes atomic.Int64
	// upstream is only read or changed by run/handleRequest. Its reader and
	// keepalive goroutines receive their own immutable connection pointer.
	upstream *websocket.Conn
}

func (s *cmtWSSession) run() {
	defer func() {
		s.cancel()
		_ = s.client.Close()
		if s.upstream != nil {
			_ = s.upstream.Close()
		}
		s.workers.Wait()
	}()
	s.workers.Add(2)
	go s.readClient()
	go s.writeClient()
	for {
		select {
		case <-s.ctx.Done():
			return
		case message := <-s.incoming:
			s.incomingBytes.Add(-int64(len(message)))
			s.handleRequest(message)
		}
	}
}

func configureWSReader(conn *websocket.Conn) {
	conn.SetReadLimit(wsReadLimit)
	_ = conn.SetReadDeadline(time.Now().Add(wsReadWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(wsReadWait))
	})
	conn.SetPingHandler(func(data string) error {
		_ = conn.SetReadDeadline(time.Now().Add(wsReadWait))
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(wsWriteWait))
	})
}

func (s *cmtWSSession) readClient() {
	defer s.workers.Done()
	defer s.cancel()
	configureWSReader(s.client)
	for {
		kind, message, err := s.client.ReadMessage()
		if err != nil {
			return
		}
		_ = s.client.SetReadDeadline(time.Now().Add(wsReadWait))
		if kind != websocket.TextMessage {
			s.closeClient(websocket.CloseUnsupportedData, "JSON-RPC requires text messages")
			return
		}
		if s.incomingBytes.Add(int64(len(message))) > wsQueueBytes {
			s.incomingBytes.Add(-int64(len(message)))
			s.closeClient(websocket.CloseTryAgainLater, "request queue full")
			return
		}
		select {
		case s.incoming <- message:
		case <-s.ctx.Done():
			s.incomingBytes.Add(-int64(len(message)))
			return
		default:
			s.incomingBytes.Add(-int64(len(message)))
			s.closeClient(websocket.CloseTryAgainLater, "request queue full")
			return
		}
	}
}

func (s *cmtWSSession) writeClient() {
	defer s.workers.Done()
	defer s.cancel()
	ticker := time.NewTicker(wsPingEvery)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case message := <-s.outgoing:
			s.outgoingBytes.Add(-int64(len(message)))
			_ = s.client.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := s.client.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			if err := s.client.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteWait)); err != nil {
				return
			}
		}
	}
}

func (s *cmtWSSession) closeClient(code int, reason string) {
	_ = s.client.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(wsWriteWait))
	s.cancel()
	_ = s.client.Close()
}

func (s *cmtWSSession) send(message []byte) {
	if s.outgoingBytes.Add(int64(len(message))) > wsQueueBytes {
		s.outgoingBytes.Add(-int64(len(message)))
		s.closeClient(websocket.CloseTryAgainLater, "response queue full")
		return
	}
	select {
	case <-s.ctx.Done():
		s.outgoingBytes.Add(-int64(len(message)))
	case s.outgoing <- message:
	default:
		s.outgoingBytes.Add(-int64(len(message)))
		// Do not drop subscription events and pretend the stream is intact.
		s.closeClient(websocket.CloseTryAgainLater, "response queue full")
	}
}

func (s *cmtWSSession) sendError(id json.RawMessage, code int, message string) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	out, _ := json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{JSONRPC: "2.0", ID: id, Error: struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{code, message}})
	s.send(out)
}

func (s *cmtWSSession) handleRequest(message []byte) {
	var request jsonRPCRequest
	if err := json.Unmarshal(message, &request); err != nil {
		// CometBFT's WebSocket transport accepts one request per frame, not
		// batches. Reject arrays rather than accidentally routing to latest.
		s.sendError(nil, -32700, "parse error: expected a JSON-RPC request object")
		return
	}
	if request.JSONRPC != "2.0" || request.Method == "" ||
		(len(request.ID) > 0 && !cache.IsJSONRPCID(request.ID)) {
		s.sendError(nil, -32600, "invalid request")
		return
	}
	if len(request.ID) == 0 || bytes.Equal(bytes.TrimSpace(request.ID), []byte("null")) {
		// Match CometBFT: notifications are ignored, including subscribe.
		return
	}

	switch request.Method {
	case "subscribe", "unsubscribe", "unsubscribe_all":
		if s.upstream == nil {
			conn, err := s.dialUpstream()
			if err != nil {
				s.sendError(request.ID, -32000, "no available subscription backend")
				return
			}
			s.upstream = conn
			s.workers.Add(2)
			go s.readUpstream(conn)
			go s.pingUpstream(conn)
		}
		_ = s.upstream.SetWriteDeadline(time.Now().Add(wsWriteWait))
		if err := s.upstream.WriteMessage(websocket.TextMessage, message); err != nil {
			s.closeClient(websocket.CloseTryAgainLater, "subscription backend disconnected; reconnect required")
		}
	default:
		r, err := http.NewRequestWithContext(s.ctx, http.MethodPost, "http://stitch/", bytes.NewReader(message))
		if err != nil {
			s.sendError(request.ID, -32603, "internal error")
			return
		}
		r.Header.Set("Content-Type", "application/json")
		response := &wsResponse{header: make(http.Header)}
		s.server.ServeHTTP(response, r)
		if s.ctx.Err() != nil {
			return
		}
		var envelope struct {
			JSONRPC string          `json:"jsonrpc"`
			Result  json.RawMessage `json:"result"`
			Error   json.RawMessage `json:"error"`
		}
		body := response.body.Bytes()
		if response.overflow || json.Unmarshal(body, &envelope) != nil || envelope.JSONRPC != "2.0" ||
			(len(envelope.Result) == 0 && (len(envelope.Error) == 0 || envelope.Error[0] != '{')) {
			s.sendError(request.ID, -32000, "upstream request failed")
			return
		}
		s.send(body)
	}
}

func (s *cmtWSSession) dialUpstream() (*websocket.Conn, error) {
	if s.server.wsSelector == nil {
		return nil, errors.New("subscription selector not configured")
	}
	candidates := s.server.wsSelector.Candidates(types.RouteKey{
		Protocol: types.ProtoRPC, Method: "subscribe", Class: types.ClassLatest,
	})
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	for _, candidate := range candidates {
		endpoint, err := url.Parse(wsurl.Normalize(candidate.Endpoint(types.ProtoRPC)))
		if err != nil || endpoint.Host == "" || (endpoint.Scheme != "ws" && endpoint.Scheme != "wss") {
			continue
		}
		endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/websocket"
		endpoint.RawPath = ""
		conn, response, err := dialer.DialContext(s.ctx, endpoint.String(), nil)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		if err == nil {
			return conn, nil
		}
		if s.ctx.Err() != nil {
			return nil, s.ctx.Err()
		}
	}
	return nil, errors.New("no available subscription backend")
}

func (s *cmtWSSession) readUpstream(conn *websocket.Conn) {
	defer s.workers.Done()
	configureWSReader(conn)
	for {
		kind, message, err := conn.ReadMessage()
		if err != nil {
			if s.ctx.Err() == nil {
				s.closeClient(websocket.CloseTryAgainLater, "subscription backend disconnected; reconnect required")
			}
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(wsReadWait))
		if kind != websocket.TextMessage || !json.Valid(message) {
			s.closeClient(websocket.CloseInternalServerErr, "invalid subscription response")
			return
		}
		s.send(message)
	}
}

func (s *cmtWSSession) pingUpstream(conn *websocket.Conn) {
	defer s.workers.Done()
	ticker := time.NewTicker(wsPingEvery)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteWait)); err != nil {
				s.closeClient(websocket.CloseTryAgainLater, "subscription backend disconnected; reconnect required")
				return
			}
		}
	}
}

// Bound the final WebSocket response buffer. The HTTP forwarder has its own
// upstream response handling; writes here never retain more than readLimit.
type wsResponse struct {
	header   http.Header
	body     bytes.Buffer
	overflow bool
}

func (w *wsResponse) Header() http.Header { return w.header }
func (w *wsResponse) WriteHeader(int)     {}
func (w *wsResponse) Write(b []byte) (int, error) {
	if len(b) > wsReadLimit-w.body.Len() {
		w.overflow = true
		return 0, errors.New("WebSocket response too large")
	}
	return w.body.Write(b)
}
