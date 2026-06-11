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

// Session is one client WebSocket connection paired with one upstream
// connection. The session lives until either side closes for good (no
// resumable subscriptions) or until ctx is cancelled.
type Session struct {
	id       string
	client   *websocket.Conn
	selector selector.Selector
	dialer   *websocket.Dialer

	mu      sync.Mutex
	subs    map[string]*Sub   // synthetic ID → sub
	upToSyn map[string]string // upstream-minted ID → synthetic ID
	pending map[string]*Sub   // our outgoing JSON-RPC id → pending sub awaiting response
	synSeq  uint64
	idSeq   atomic.Uint64

	upstream  atomic.Pointer[websocket.Conn]
	upBackend atomic.Value // string

	clientWriteMu   sync.Mutex
	upstreamWriteMu sync.Mutex

	closed atomic.Bool
	done   chan struct{} // closed when Run exits; releases clientReader sends
}

// Sub is one active subscription owned by a session.
type Sub struct {
	SyntheticID string
	UpstreamID  string // empty until upstream responds; mutated on resume
	Kind        Kind
	Params      json.RawMessage // original eth_subscribe params, replayed on resume
	Cursor      Cursor
	ClientID    json.RawMessage // last client-supplied id used for the subscribe response
	Resumable   bool
}

// SessionConfig configures a session at construction.
type SessionConfig struct {
	Selector         selector.Selector
	Dialer           *websocket.Dialer
	HandshakeTimeout time.Duration
}

// NewSession constructs a session pinned to the given client connection.
// Run() drives it until the client or all upstreams give up.
func NewSession(client *websocket.Conn, cfg SessionConfig) *Session {
	d := cfg.Dialer
	if d == nil {
		d = &websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	}
	return &Session{
		id:       runtime.NewRequestID(),
		client:   client,
		selector: cfg.Selector,
		dialer:   d,
		subs:     make(map[string]*Sub),
		upToSyn:  make(map[string]string),
		pending:  make(map[string]*Sub),
		done:     make(chan struct{}),
	}
}

// Run blocks until the session terminates. Returns the terminal cause.
func (s *Session) Run(ctx context.Context) error {
	defer s.closeClient(websocket.CloseGoingAway, "session ending")
	defer metrics.SubscriptionsActive.WithLabelValues(string(types.ProtoEthWS), "session").Dec()
	defer close(s.done) // every return stops draining clientCh; release the reader
	metrics.SubscriptionsActive.WithLabelValues(string(types.ProtoEthWS), "session").Inc()

	clientCh := make(chan []byte, 32)
	clientErrCh := make(chan error, 1)
	go s.clientReader(ctx, clientCh, clientErrCh)

	for {
		// Dial an upstream.
		ok := s.dialUpstream(ctx)
		if !ok {
			return errors.New("no eligible upstream")
		}

		// On every (re)connect, replay resumable subscriptions.
		s.replaySubs()

		upDone := make(chan error, 1)
		go func() {
			upDone <- s.upstreamReader(ctx)
		}()

		// Forward client → upstream until either side dies.
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
					log.FromCtx(ctx).Debug("client frame route failed", "err", err.Error())
					s.tearDownUpstream()
					<-upDone
					return err
				}
			case err := <-clientErrCh:
				s.tearDownUpstream()
				<-upDone
				return err
			case err := <-upDone:
				_ = err
				log.FromCtx(ctx).Info("upstream gone, evaluating resume",
					"backend", s.upBackend.Load(),
					"resumable_subs", s.countResumable(),
				)
				if !s.hasResumableSubs() {
					return nil
				}
				metrics.SubscriptionResumes.WithLabelValues("upstream_close").Inc()
				break forward // loop reconnects
			}
		}
	}
}

// closeClient writes a close frame and closes the underlying conn once.
func (s *Session) closeClient(code int, msg string) {
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

// dialUpstream picks the next selector candidate and opens a WS to it.
// Returns false if every candidate fails.
func (s *Session) dialUpstream(ctx context.Context) bool {
	candidates := s.selector.Candidates(types.RouteKey{
		Protocol: types.ProtoEthWS,
		Method:   "subscribe_session",
		Class:    types.ClassLatest,
	})
	for _, b := range candidates {
		ep := b.Endpoint(types.ProtoEthWS)
		if ep == "" {
			continue
		}
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, _, err := s.dialer.DialContext(dialCtx, normalizeWS(ep), nil)
		cancel()
		if err != nil {
			log.FromCtx(ctx).Warn("session: upstream dial failed", "backend", b.Name, "err", err.Error())
			continue
		}
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		})
		s.upstream.Store(conn)
		s.upBackend.Store(b.Name)
		log.FromCtx(ctx).Info("session: upstream connected", "session_id", s.id, "backend", b.Name)
		return true
	}
	return false
}

func (s *Session) tearDownUpstream() {
	if c := s.upstream.Swap(nil); c != nil {
		_ = c.Close()
	}
}

// clientReader pumps frames from the client into clientCh until close.
// Sends race s.done: once Run returns nothing drains clientCh, and a conn
// close only unblocks ReadMessage — a full buffer would otherwise strand
// this goroutine on the send forever.
func (s *Session) clientReader(_ context.Context, clientCh chan<- []byte, errCh chan<- error) {
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

// upstreamReader pumps frames from the current upstream and dispatches
// them as either subscribe responses, notifications, or pass-through
// JSON-RPC responses.
func (s *Session) upstreamReader(ctx context.Context) error {
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

// routeClientFrame inspects an incoming client frame and either:
//   - intercepts an eth_subscribe (records pending) and forwards a
//     stitch-issued copy to upstream
//   - intercepts an eth_unsubscribe by synthetic ID and rewrites it
//   - or forwards verbatim
func (s *Session) routeClientFrame(msg []byte) error {
	var probe struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	_ = json.Unmarshal(msg, &probe)

	switch probe.Method {
	case "eth_subscribe":
		return s.handleClientSubscribe(probe.ID, probe.Params)
	case "eth_unsubscribe":
		return s.handleClientUnsubscribe(probe.ID, probe.Params)
	default:
		return s.upstreamWrite(msg)
	}
}

// handleClientSubscribe registers a pending subscription with a fresh
// stitch JSON-RPC id, forwards to upstream. On the upstream response,
// we'll mint the synthetic ID and reply to the client.
func (s *Session) handleClientSubscribe(clientID, params json.RawMessage) error {
	kind := readSubscribeKind(params)
	syn := s.mintSynthetic()
	internalID := s.nextID()

	sub := &Sub{
		SyntheticID: syn,
		Kind:        kind,
		Params:      append([]byte(nil), params...),
		ClientID:    append([]byte(nil), clientID...),
		Resumable:   kind.Resumable(),
	}

	s.mu.Lock()
	s.subs[syn] = sub
	s.pending[internalID] = sub
	s.mu.Unlock()

	out, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      internalID,
		"method":  "eth_subscribe",
		"params":  json.RawMessage(params),
	})
	if err != nil {
		return err
	}
	return s.upstreamWrite(out)
}

// handleClientUnsubscribe maps the synthetic ID back to upstream, sends
// the upstream-form unsubscribe, replies "true" to the client, and forgets
// the sub.
func (s *Session) handleClientUnsubscribe(clientID, params json.RawMessage) error {
	id := firstStringParam(params)
	s.mu.Lock()
	sub, ok := s.subs[id]
	if ok {
		delete(s.subs, id)
		if sub.UpstreamID != "" {
			delete(s.upToSyn, sub.UpstreamID)
		}
	}
	s.mu.Unlock()

	if !ok {
		return s.clientReply(clientID, false)
	}
	if sub.UpstreamID != "" {
		out, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      s.nextID(),
			"method":  "eth_unsubscribe",
			"params":  []string{sub.UpstreamID},
		})
		_ = s.upstreamWrite(out)
	}
	return s.clientReply(clientID, true)
}

// routeUpstreamFrame inspects a frame from upstream:
//   - notification: translate id, dedup, forward
//   - response with our internal id: bind synthetic, reply to client
//   - other response: forward verbatim (may be eth_call etc.)
func (s *Session) routeUpstreamFrame(_ context.Context, msg []byte) error {
	var probe struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
		Result json.RawMessage `json:"result"`
	}
	_ = json.Unmarshal(msg, &probe)

	if probe.Method == "eth_subscription" {
		return s.handleUpstreamNotification(msg)
	}
	if len(probe.ID) > 0 {
		idStr := strings.Trim(string(probe.ID), `"`)
		s.mu.Lock()
		pending, ok := s.pending[idStr]
		if ok {
			delete(s.pending, idStr)
		}
		s.mu.Unlock()
		if ok {
			return s.handleUpstreamSubscribeResp(pending, probe.Result)
		}
	}
	return s.clientWrite(msg)
}

func (s *Session) handleUpstreamNotification(msg []byte) error {
	// Parse to find the upstream sub ID.
	var env struct {
		Params struct {
			Subscription string `json:"subscription"`
		} `json:"params"`
	}
	if err := json.Unmarshal(msg, &env); err != nil {
		return s.clientWrite(msg)
	}
	upID := env.Params.Subscription

	s.mu.Lock()
	syn, ok := s.upToSyn[upID]
	var sub *Sub
	if ok {
		sub = s.subs[syn]
	}
	s.mu.Unlock()
	if !ok || sub == nil {
		metrics.SubscriptionDroppedNotifs.WithLabelValues(string(types.ProtoEthWS), "unknown_sub").Inc()
		return nil // unknown sub — drop
	}

	// Dedup against cursor.
	parsed, _ := ParseEthNotification(msg, sub.Kind)
	if !parsed.Cursor.IsZero() && parsed.Cursor.LessEq(sub.Cursor) && sub.Cursor != parsed.Cursor {
		// Strictly behind cursor → drop (resume duplicate).
		return nil
	}
	if parsed.Cursor == sub.Cursor && !sub.Cursor.IsZero() {
		return nil
	}

	rewritten, ok := RewriteSubscriptionID(msg, syn)
	if !ok {
		rewritten = msg
	}

	if !parsed.Cursor.IsZero() {
		s.mu.Lock()
		sub.Cursor = parsed.Cursor
		s.mu.Unlock()
	}
	return s.clientWrite(rewritten)
}

func (s *Session) handleUpstreamSubscribeResp(sub *Sub, result json.RawMessage) error {
	var upID string
	if err := json.Unmarshal(result, &upID); err != nil {
		// Subscribe failed — propagate error to client.
		return s.clientReplyError(sub.ClientID, -32603, "upstream subscribe failed")
	}
	s.mu.Lock()
	sub.UpstreamID = upID
	s.upToSyn[upID] = sub.SyntheticID
	clientID := append(json.RawMessage(nil), sub.ClientID...)
	syn := sub.SyntheticID
	first := sub.Cursor.IsZero() // first-time subscribe (not a re-issue)
	s.mu.Unlock()

	if !first {
		return nil // resume: don't re-reply; the original response already went out
	}
	return s.clientReplyResult(clientID, syn)
}

// replaySubs is called after a (re)connect. For each resumable sub,
// re-issue eth_subscribe with the original params; the upstream's
// response binds a fresh upstream id. The stale upstream-id mapping is
// purged before re-issue so a late notification from the dead upstream
// can't sneak through.
func (s *Session) replaySubs() {
	s.mu.Lock()
	subs := make([]*Sub, 0, len(s.subs))
	for _, sub := range s.subs {
		if sub.Resumable {
			if sub.UpstreamID != "" {
				delete(s.upToSyn, sub.UpstreamID)
				sub.UpstreamID = ""
			}
			subs = append(subs, sub)
		}
	}
	s.mu.Unlock()

	for _, sub := range subs {
		internalID := s.nextID()
		s.mu.Lock()
		s.pending[internalID] = sub
		s.mu.Unlock()
		out, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      internalID,
			"method":  "eth_subscribe",
			"params":  json.RawMessage(sub.Params),
		})
		if err := s.upstreamWrite(out); err != nil {
			return
		}
	}
}

func (s *Session) hasResumableSubs() bool {
	return s.countResumable() > 0
}

func (s *Session) countResumable() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, sub := range s.subs {
		if sub.Resumable {
			n++
		}
	}
	return n
}

func (s *Session) mintSynthetic() string {
	n := atomic.AddUint64(&s.synSeq, 1)
	return fmt.Sprintf("0x%016x", n)
}

func (s *Session) nextID() string {
	return fmt.Sprintf("stitch_%d", s.idSeq.Add(1))
}

// upstreamWrite is serialized — gorilla forbids concurrent writers on a
// single connection.
func (s *Session) upstreamWrite(msg []byte) error {
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

func (s *Session) clientWrite(msg []byte) error {
	s.clientWriteMu.Lock()
	defer s.clientWriteMu.Unlock()
	if err := s.client.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return s.client.WriteMessage(websocket.TextMessage, msg)
}

func (s *Session) clientReplyResult(id json.RawMessage, result string) error {
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(id),
		"result":  result,
	})
	return s.clientWrite(out)
}

func (s *Session) clientReply(id json.RawMessage, result bool) error {
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(id),
		"result":  result,
	})
	return s.clientWrite(out)
}

func (s *Session) clientReplyError(id json.RawMessage, code int, msg string) error {
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(id),
		"error": map[string]any{
			"code":    code,
			"message": msg,
		},
	})
	return s.clientWrite(out)
}

// readSubscribeKind reads params[0] of an eth_subscribe request.
func readSubscribeKind(params json.RawMessage) Kind {
	var arr []json.RawMessage
	if err := json.Unmarshal(params, &arr); err != nil || len(arr) == 0 {
		return KindUnknown
	}
	var s string
	if err := json.Unmarshal(arr[0], &s); err != nil {
		return KindUnknown
	}
	return ParseEthKind(s)
}

func firstStringParam(params json.RawMessage) string {
	var arr []json.RawMessage
	if err := json.Unmarshal(params, &arr); err != nil || len(arr) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(arr[0], &s); err != nil {
		return ""
	}
	return s
}

func normalizeWS(s string) string {
	switch {
	case strings.HasPrefix(s, "ws://"), strings.HasPrefix(s, "wss://"):
		return s
	case strings.HasPrefix(s, "http://"):
		return "ws://" + strings.TrimPrefix(s, "http://")
	case strings.HasPrefix(s, "https://"):
		return "wss://" + strings.TrimPrefix(s, "https://")
	default:
		return s
	}
}

// Backend names the currently bound upstream — used by tests.
func (s *Session) Backend() string {
	v, _ := s.upBackend.Load().(string)
	return v
}
