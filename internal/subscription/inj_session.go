package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/runtime"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/types"
)

// InjSession is one /injstream-ws client connection paired with one
// upstream connection. Compared to the eth_ws Session:
//
//   - JSON-RPC method names are subscribe/unsubscribe (not eth_*).
//   - Subscriptions are keyed by the client-supplied subscription_id;
//     stitch does not mint synthetic ids — but it DOES rewrite the
//     JSON-RPC `id` on notifications when re-issuing on resume, so the
//     client sees a stable id.
//   - Cursor = block_height (single int) extracted from the JSON
//     notification result. ChainStream guarantees monotonic delivery, so
//     dedup is straightforward height comparison.
type InjSession struct {
	id       string
	client   *websocket.Conn
	selector selector.Selector
	dialer   *websocket.Dialer

	mu      sync.Mutex
	subs    map[string]*InjSub // subscription_id → sub
	pending map[string]*InjSub // our internal JSON-RPC id → sub awaiting upstream ack
	idSeq   atomic.Uint64

	upstream  atomic.Pointer[websocket.Conn]
	upBackend atomic.Value // string

	clientWriteMu   sync.Mutex
	upstreamWriteMu sync.Mutex
	closed          atomic.Bool
	done            chan struct{} // closed when Run exits; releases clientReader sends
}

// InjSub captures one /injstream-ws subscription's full replay state.
type InjSub struct {
	SubscriptionID  string          // client-chosen, used in upstream subscribe
	ClientJSONRPCID json.RawMessage // original client subscribe id; rewritten onto notifications
	Filter          json.RawMessage // for replay on resume
	Cursor          Cursor          // last delivered block_height
	UpstreamID      string          // current id used in our outgoing subscribe; mutated on resume
}

// InjSessionConfig configures a session at construction.
type InjSessionConfig struct {
	Selector         selector.Selector
	Dialer           *websocket.Dialer
	HandshakeTimeout time.Duration
}

// NewInjSession constructs a /injstream-ws session.
func NewInjSession(client *websocket.Conn, cfg InjSessionConfig) *InjSession {
	d := cfg.Dialer
	if d == nil {
		d = &websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	}
	return &InjSession{
		id:       runtime.NewRequestID(),
		client:   client,
		selector: cfg.Selector,
		dialer:   d,
		subs:     make(map[string]*InjSub),
		pending:  make(map[string]*InjSub),
		done:     make(chan struct{}),
	}
}

// Run blocks until terminated. Returns the terminal cause.
func (s *InjSession) Run(ctx context.Context) error {
	defer s.closeClient(websocket.CloseGoingAway, "session ending")
	defer metrics.SubscriptionsActive.WithLabelValues(string(types.ProtoChainStream), "inj_ws").Dec()
	defer close(s.done) // every return stops draining clientCh; release the reader
	metrics.SubscriptionsActive.WithLabelValues(string(types.ProtoChainStream), "inj_ws").Inc()

	clientCh := make(chan []byte, 32)
	clientErrCh := make(chan error, 1)
	go s.clientReader(ctx, clientCh, clientErrCh)

	for {
		if !s.dialUpstream(ctx) {
			return errors.New("no eligible upstream for /injstream-ws")
		}
		s.replaySubs(ctx)

		upDone := make(chan error, 1)
		go func() { upDone <- s.upstreamReader(ctx) }()

	forward:
		for {
			select {
			case <-ctx.Done():
				s.tearDownUpstream()
				<-upDone
				return ctx.Err()
			case msg, ok := <-clientCh:
				if !ok {
					s.tearDownUpstream()
					<-upDone
					return nil
				}
				if err := s.routeClientFrame(msg); err != nil {
					log.FromCtx(ctx).Debug("inj_ws: client frame route failed", "err", err.Error())
					s.tearDownUpstream()
					<-upDone
					return err
				}
			case err := <-clientErrCh:
				s.tearDownUpstream()
				<-upDone
				return err
			case <-upDone:
				log.FromCtx(ctx).Info("inj_ws: upstream gone, evaluating resume",
					"backend", s.upBackend.Load(),
					"subs", s.countSubs(),
				)
				if s.countSubs() == 0 {
					return nil
				}
				metrics.SubscriptionResumes.WithLabelValues("inj_ws_upstream_close").Inc()
				break forward
			}
		}
	}
}

func (s *InjSession) closeClient(code int, msg string) {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.clientWriteMu.Lock()
	_ = s.client.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(code, msg),
		time.Now().Add(2*time.Second),
	)
	s.clientWriteMu.Unlock()
	_ = s.client.Close()
}

// dialUpstream picks the next selector candidate and opens a WS to its
// /injstream-ws endpoint. The endpoint key reuses ProtoChainStream — a
// chainstream-capable backend is assumed to expose /injstream-ws on the
// same host but a different port; operators put the WS URL in the
// `chainstream` endpoint slot when they want it routable by stitch.
func (s *InjSession) dialUpstream(ctx context.Context) bool {
	candidates := s.selector.Candidates(types.RouteKey{
		Protocol: types.ProtoChainStream,
		Method:   "inj_ws_session",
		Class:    types.ClassLatest,
	})
	for _, b := range candidates {
		ep := b.Endpoint(types.ProtoChainStream)
		if ep == "" {
			continue
		}
		addr := injWSURL(ep)
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, _, err := s.dialer.DialContext(dialCtx, addr, nil)
		cancel()
		if err != nil {
			log.FromCtx(ctx).Warn("inj_ws: upstream dial failed", "backend", b.Name, "err", err.Error())
			continue
		}
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		})
		s.upstream.Store(conn)
		s.upBackend.Store(b.Name)
		log.FromCtx(ctx).Info("inj_ws: upstream connected", "session_id", s.id, "backend", b.Name)
		return true
	}
	return false
}

func (s *InjSession) tearDownUpstream() {
	if c := s.upstream.Swap(nil); c != nil {
		_ = c.Close()
	}
}

// clientReader pumps frames from the client into clientCh until close.
// Sends race s.done: once Run returns nothing drains clientCh, and a conn
// close only unblocks ReadMessage — a full buffer would otherwise strand
// this goroutine on the send forever.
func (s *InjSession) clientReader(_ context.Context, clientCh chan<- []byte, errCh chan<- error) {
	defer close(clientCh)
	for {
		_, msg, err := s.client.ReadMessage()
		if err != nil {
			errCh <- err // cap-1, single send ever — never blocks
			return
		}
		select {
		case clientCh <- msg:
		case <-s.done:
			return
		}
	}
}

func (s *InjSession) upstreamReader(ctx context.Context) error {
	conn := s.upstream.Load()
	if conn == nil {
		return errors.New("no upstream")
	}
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if err := s.routeUpstreamFrame(ctx, msg); err != nil {
			return err
		}
	}
}

func (s *InjSession) routeClientFrame(msg []byte) error {
	var probe struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(msg, &probe); err != nil {
		// Forward verbatim if we can't parse; let upstream handle errors.
		return s.upstreamWrite(msg)
	}
	switch probe.Method {
	case "subscribe":
		return s.handleSubscribe(probe.ID, probe.Params)
	case "unsubscribe":
		return s.handleUnsubscribe(probe.ID, probe.Params)
	default:
		return s.upstreamWrite(msg)
	}
}

// handleSubscribe registers the sub locally, reissues to upstream with
// our internal JSON-RPC id, and awaits the ack.
func (s *InjSession) handleSubscribe(clientID, params json.RawMessage) error {
	sp, ok := ParseInjSubscribeParams(params)
	if !ok {
		return s.clientReplyError(clientID, -32602, "subscribe params: missing subscription_id or filter")
	}
	internalID := s.nextID()
	sub := &InjSub{
		SubscriptionID:  sp.SubscriptionID,
		ClientJSONRPCID: append(json.RawMessage(nil), clientID...),
		Filter:          append(json.RawMessage(nil), sp.Filter...),
		UpstreamID:      internalID,
	}
	s.mu.Lock()
	s.subs[sp.SubscriptionID] = sub
	s.pending[internalID] = sub
	s.mu.Unlock()

	out, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      internalID,
		"method":  "subscribe",
		"params":  map[string]any{"subscription_id": sp.SubscriptionID, "filter": json.RawMessage(sp.Filter)},
	})
	if err != nil {
		return err
	}
	return s.upstreamWrite(out)
}

func (s *InjSession) handleUnsubscribe(clientID, params json.RawMessage) error {
	up, ok := ParseInjUnsubscribeParams(params)
	if !ok {
		return s.clientReplyError(clientID, -32602, "unsubscribe params: missing subscription_id")
	}
	s.mu.Lock()
	delete(s.subs, up.SubscriptionID)
	s.mu.Unlock()
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      s.nextID(),
		"method":  "unsubscribe",
		"params":  map[string]any{"subscription_id": up.SubscriptionID},
	})
	if err := s.upstreamWrite(out); err != nil {
		return err
	}
	// Reply success to client.
	out2, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(clientID),
		"result":  "success",
	})
	return s.clientWrite(out2)
}

func (s *InjSession) routeUpstreamFrame(_ context.Context, msg []byte) error {
	var probe struct {
		ID     json.RawMessage `json:"id"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(msg, &probe); err != nil {
		return s.clientWrite(msg)
	}
	resTrim := bytesTrim(probe.Result)

	// Subscribe ack: result is the literal string "success".
	if len(resTrim) > 0 && resTrim[0] == '"' {
		idStr := unquoteID(probe.ID)
		s.mu.Lock()
		sub, ok := s.pending[idStr]
		if ok {
			delete(s.pending, idStr)
		}
		s.mu.Unlock()
		if !ok {
			// Unknown id — pass through.
			return s.clientWrite(msg)
		}
		// First-time subscribe: forward ack with the client's original id.
		// Re-issue (resume): silently absorb.
		first := sub.Cursor.IsZero()
		if !first {
			return nil
		}
		ack, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      json.RawMessage(sub.ClientJSONRPCID),
			"result":  "success",
		})
		return s.clientWrite(ack)
	}

	// Notification: result is an object with block_height.
	notif, ok := ParseInjNotification(msg)
	if !ok {
		// Unknown frame shape — forward verbatim.
		return s.clientWrite(msg)
	}
	idStr := unquoteID(notif.ID)

	s.mu.Lock()
	var sub *InjSub
	// Notifications carry whatever id we used to subscribe. The sub's
	// UpstreamID is the current outgoing id (mutated on resume), so we
	// match against that. Fall back to the pending map for the brief
	// window between subscribe write and ack drain.
	for _, candidate := range s.subs {
		if candidate.UpstreamID == idStr {
			sub = candidate
			break
		}
	}
	if sub == nil {
		if pendingSub, ok := s.pending[idStr]; ok {
			sub = pendingSub
		}
	}
	s.mu.Unlock()
	if sub == nil {
		metrics.SubscriptionDroppedNotifs.WithLabelValues(string(types.ProtoChainStream), "unknown_sub").Inc()
		return nil // drop unknown
	}

	// Dedup against cursor.
	if !notif.Cursor.IsZero() && notif.Cursor.LessEq(sub.Cursor) {
		return nil
	}

	// Rewrite id back to the client's original.
	rewritten, ok := RewriteInjNotificationID(msg, sub.ClientJSONRPCID)
	if !ok {
		rewritten = msg
	}

	if !notif.Cursor.IsZero() {
		s.mu.Lock()
		if notif.Cursor.LessEq(sub.Cursor) {
			s.mu.Unlock()
			return nil
		}
		sub.Cursor = notif.Cursor
		s.mu.Unlock()
	}
	return s.clientWrite(rewritten)
}

// replaySubs re-issues every active subscription on the freshly-dialed
// upstream. The upstream replies "success" (which routeUpstreamFrame
// silently absorbs because Cursor is non-zero). Subsequent notifications
// match via the internal id we attach below.
func (s *InjSession) replaySubs(ctx context.Context) {
	s.mu.Lock()
	subs := make([]*InjSub, 0, len(s.subs))
	for _, sub := range s.subs {
		subs = append(subs, sub)
	}
	s.mu.Unlock()

	for _, sub := range subs {
		internalID := s.nextID()
		s.mu.Lock()
		// Forget the previous upstream id; new id replaces it.
		sub.UpstreamID = internalID
		s.pending[internalID] = sub
		s.mu.Unlock()
		out, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      internalID,
			"method":  "subscribe",
			"params":  map[string]any{"subscription_id": sub.SubscriptionID, "filter": json.RawMessage(sub.Filter)},
		})
		if err := s.upstreamWrite(out); err != nil {
			log.FromCtx(ctx).Warn("inj_ws: replay write failed", "err", err.Error())
			return
		}
	}
}

func (s *InjSession) countSubs() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs)
}

func (s *InjSession) nextID() string {
	return fmt.Sprintf("stitch_inj_%d", s.idSeq.Add(1))
}

func (s *InjSession) upstreamWrite(msg []byte) error {
	conn := s.upstream.Load()
	if conn == nil {
		return errors.New("no upstream")
	}
	s.upstreamWriteMu.Lock()
	defer s.upstreamWriteMu.Unlock()
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, msg)
}

func (s *InjSession) clientWrite(msg []byte) error {
	s.clientWriteMu.Lock()
	defer s.clientWriteMu.Unlock()
	if err := s.client.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return s.client.WriteMessage(websocket.TextMessage, msg)
}

func (s *InjSession) clientReplyError(id json.RawMessage, code int, msg string) error {
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(id),
		"error":   map[string]any{"code": code, "message": msg},
	})
	return s.clientWrite(out)
}

// injWSURL normalizes the operator-provided endpoint URL. ChainStream
// gRPC endpoints are bare host:port; /injstream-ws is HTTP+WS so we need
// a ws:// or wss:// scheme. If the operator provides one of those
// already, use as-is; otherwise prefix ws:// and append /injstream-ws.
func injWSURL(s string) string {
	switch {
	case len(s) > 5 && s[:5] == "wss:/":
		return s
	case len(s) > 5 && s[:5] == "ws://":
		return s
	case len(s) > 7 && s[:7] == "https:/":
		return "wss://" + s[8:] + "/injstream-ws"
	case len(s) > 7 && s[:7] == "http://":
		return "ws://" + s[7:] + "/injstream-ws"
	}
	// Bare host:port → assume insecure ws.
	return "ws://" + s + "/injstream-ws"
}

func bytesTrim(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\n' || b[0] == '\r') {
		b = b[1:]
	}
	return b
}

// unquoteID converts a JSON-RPC id (which may be a number, a quoted
// string, or null) into a comparable string key. We treat "stitch_inj_1"
// and stitch_inj_1 as the same id so the pending map keys can be plain
// Go strings.
func unquoteID(id json.RawMessage) string {
	s := strings.TrimSpace(string(id))
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		var u string
		if err := json.Unmarshal([]byte(s), &u); err == nil {
			return u
		}
		return s[1 : len(s)-1]
	}
	return s
}
