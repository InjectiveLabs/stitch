package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/types"
)

// Hub multiplexes /injstream-ws upstreams. Multiple client sessions that
// subscribe to filters with the same canonical key share one upstream
// connection; the hub fans events out to each client's send channel.
//
// On upstream death the hub re-dials, replays the subscribe, and dedups
// any events at-or-behind the shared cursor. Clients see no
// interruption beyond the millisecond-level reconnect window.
//
// Phase 5d scope: /injstream-ws JSON-RPC over WS only. The same shape
// will work for eth_ws (JSON canonicalization) and for chainstream gRPC
// (proto canonicalization) once those adapters land.
type Hub struct {
	selector selector.Selector
	dialer   *websocket.Dialer

	mu        sync.Mutex
	upstreams map[string]*hubUpstream

	// SlowConsumer is the policy applied when a subscriber's Out channel
	// is full. "drop" (default) drops the oldest queued event for that
	// client only. "disconnect" closes the subscriber. "backpressure"
	// blocks the upstream — only useful for log-indexer style consumers.
	SlowConsumer string

	// SendBufSize sets the per-subscriber send-channel capacity.
	SendBufSize int
}

func NewHub(sel selector.Selector, dialer *websocket.Dialer) *Hub {
	if dialer == nil {
		dialer = &websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	}
	return &Hub{
		selector:     sel,
		dialer:       dialer,
		upstreams:    make(map[string]*hubUpstream),
		SlowConsumer: "drop",
		SendBufSize:  64,
	}
}

// Subscribe attaches a new client to the upstream for filter's canonical
// key. Caller-supplied subscription ID and synthetic ID are stored on
// the Subscriber and used to rewrite outgoing notifications.
//
// On error, the caller should report the error to its client; on
// success, the caller spawns a goroutine that pumps Subscriber.Out into
// the client connection until Subscriber.Done() fires.
func (h *Hub) Subscribe(ctx context.Context, filter json.RawMessage, clientSubscriptionID string, clientJSONRPCID json.RawMessage) (*Subscriber, error) {
	key, err := CanonicalKey(filter)
	if err != nil {
		return nil, err
	}

	sub := &Subscriber{
		Out:                  make(chan []byte, h.SendBufSize),
		ClientSubscriptionID: clientSubscriptionID,
		ClientJSONRPCID:      append(json.RawMessage(nil), clientJSONRPCID...),
		done:                 make(chan struct{}),
		hub:                  h,
	}

	h.mu.Lock()
	up, ok := h.upstreams[key]
	created := !ok
	if created {
		up = newHubUpstream(h, key, filter)
		h.upstreams[key] = up
	}
	sub.upstream = up
	// Attach BEFORE the goroutine starts so the run loop's first
	// hasClients() check sees us.
	up.attach(sub)
	h.mu.Unlock()

	if created {
		go up.run(context.Background())
	}
	metrics.SubscriptionsActive.WithLabelValues("hub", "client").Inc()
	return sub, nil
}

// removeUpstream is called by hubUpstream when its last client detaches.
func (h *Hub) removeUpstream(key string) {
	h.mu.Lock()
	delete(h.upstreams, key)
	h.mu.Unlock()
}

// UpstreamCount is exposed for tests / admin.
func (h *Hub) UpstreamCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.upstreams)
}

// Subscriber is a hub-attached client subscription. The session pumps
// Out → client connection until Done() fires.
type Subscriber struct {
	Out                  chan []byte
	ClientSubscriptionID string          // /injstream-ws subscribe.subscription_id
	ClientJSONRPCID      json.RawMessage // JSON-RPC envelope id; rewritten onto every notification

	mu        sync.Mutex
	closed    bool
	done      chan struct{}
	hub       *Hub
	upstream  *hubUpstream
	cursor    int64
	dropCount atomic.Int64
}

// Done signals when the subscription has been terminated (by the
// session via Detach, by the upstream giving up, or by slow-consumer
// disconnect policy).
func (s *Subscriber) Done() <-chan struct{} { return s.done }

// Detach removes the subscriber from its upstream. Idempotent.
func (s *Subscriber) Detach() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.done)
	s.mu.Unlock()

	s.upstream.detach(s)
	metrics.SubscriptionsActive.WithLabelValues("hub", "client").Dec()
}

// DropCount exposes the per-subscriber drop count for tests.
func (s *Subscriber) DropCount() int64 { return s.dropCount.Load() }

// hubUpstream owns one upstream WS connection that may be shared by N
// subscribers.
type hubUpstream struct {
	hub    *Hub
	key    string
	filter json.RawMessage

	mu         sync.Mutex
	cursor     int64
	conn       *websocket.Conn
	clients    []*Subscriber
	pending    map[string]struct{} // outgoing subscribe ids awaiting ack
	upstreamID string              // current id used in subscribe to upstream
	detachCh   chan struct{}
	idSeq      atomic.Uint64
	closed     bool
}

func newHubUpstream(h *Hub, key string, filter json.RawMessage) *hubUpstream {
	return &hubUpstream{
		hub:      h,
		key:      key,
		filter:   append(json.RawMessage(nil), filter...),
		pending:  make(map[string]struct{}),
		detachCh: make(chan struct{}),
	}
}

func (u *hubUpstream) attach(s *Subscriber) {
	u.mu.Lock()
	u.clients = append(u.clients, s)
	u.mu.Unlock()
}

func (u *hubUpstream) detach(s *Subscriber) {
	u.mu.Lock()
	for i, c := range u.clients {
		if c == s {
			u.clients = append(u.clients[:i], u.clients[i+1:]...)
			break
		}
	}
	empty := len(u.clients) == 0
	u.mu.Unlock()
	if empty {
		select {
		case u.detachCh <- struct{}{}:
		default:
		}
	}
}

// run is the upstream's lifecycle goroutine. It dials, sends the
// subscribe, fans out notifications until either every client has
// detached or the upstream is unrecoverable.
func (u *hubUpstream) run(ctx context.Context) {
	defer func() {
		u.mu.Lock()
		u.closed = true
		conn := u.conn
		u.conn = nil
		u.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		u.hub.removeUpstream(u.key)
		log.L().Debug("hub upstream closed", "key", u.key)
	}()

	for {
		if !u.hasClients() {
			return
		}
		if !u.dial(ctx) {
			u.failAllClients("no eligible upstream")
			return
		}
		err := u.runUntilFailure(ctx)
		if err == nil || u.shouldExit() {
			return
		}
		log.L().Info("hub: upstream lost, resuming", "key", u.key, "err", err.Error())
		metrics.SubscriptionResumes.WithLabelValues("hub_upstream_close").Inc()
	}
}

func (u *hubUpstream) hasClients() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.clients) > 0
}

func (u *hubUpstream) shouldExit() bool {
	select {
	case <-u.detachCh:
		return !u.hasClients()
	default:
		return !u.hasClients()
	}
}

// dial picks the next selector candidate and opens a WS to its
// /injstream-ws endpoint. Returns false if nothing is dialable.
func (u *hubUpstream) dial(ctx context.Context) bool {
	candidates := u.hub.selector.Candidates(types.RouteKey{
		Protocol: types.ProtoChainStream,
		Method:   "hub_inj_ws",
		Class:    types.ClassLatest,
	})
	for _, b := range candidates {
		ep := b.Endpoint(types.ProtoChainStream)
		if ep == "" {
			continue
		}
		addr := injWSURLNormalize(ep)
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, _, err := u.hub.dialer.DialContext(dialCtx, addr, nil)
		cancel()
		if err != nil {
			log.L().Warn("hub: upstream dial failed", "key", u.key, "backend", b.Name, "err", err.Error())
			continue
		}
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		})
		u.mu.Lock()
		u.conn = conn
		u.mu.Unlock()
		log.L().Info("hub: upstream connected", "key", u.key, "backend", b.Name)
		return true
	}
	return false
}

// runUntilFailure issues subscribe and fans out notifications.
// Returns nil if the upstream finished cleanly (last client gone) or an
// error if the connection died.
func (u *hubUpstream) runUntilFailure(_ context.Context) error {
	if err := u.issueSubscribe(); err != nil {
		return err
	}

	u.mu.Lock()
	conn := u.conn
	u.mu.Unlock()
	if conn == nil {
		return errors.New("no upstream conn")
	}

	for {
		// Stop if all clients gone.
		if !u.hasClients() {
			return nil
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		u.handleFrame(msg)
	}
}

func (u *hubUpstream) issueSubscribe() error {
	internalID := fmt.Sprintf("hub_%s_%d", u.key, u.idSeq.Add(1))
	u.mu.Lock()
	u.upstreamID = internalID
	u.pending[internalID] = struct{}{}
	conn := u.conn
	u.mu.Unlock()

	if conn == nil {
		return errors.New("no conn")
	}

	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      internalID,
		"method":  "subscribe",
		"params":  map[string]any{"subscription_id": "hub:" + u.key, "filter": json.RawMessage(u.filter)},
	})
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, out)
}

func (u *hubUpstream) handleFrame(msg []byte) {
	var probe struct {
		ID     json.RawMessage `json:"id"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(msg, &probe); err != nil {
		return
	}
	res := bytes_TrimSpace(probe.Result)
	// Subscribe ack: result is a string ("success"). Drain pending; do
	// NOT forward (the per-client ack was already sent at attach time).
	if len(res) > 0 && res[0] == '"' {
		idStr := unquoteID(probe.ID)
		u.mu.Lock()
		delete(u.pending, idStr)
		u.mu.Unlock()
		return
	}
	// Notification: parse cursor, dedup, fan out.
	notif, ok := ParseInjNotification(msg)
	if !ok {
		return
	}
	u.mu.Lock()
	if notif.Cursor.Height > 0 && notif.Cursor.Height <= u.cursor {
		u.mu.Unlock()
		return
	}
	if notif.Cursor.Height > 0 {
		u.cursor = notif.Cursor.Height
	}
	clients := append([]*Subscriber(nil), u.clients...)
	u.mu.Unlock()

	for _, c := range clients {
		u.fanOut(c, msg)
	}
}

// fanOut delivers the notification to one subscriber, applying the slow-
// consumer policy when the channel is full.
func (u *hubUpstream) fanOut(s *Subscriber, msg []byte) {
	rewritten, ok := RewriteInjNotificationID(msg, s.ClientJSONRPCID)
	if !ok {
		rewritten = msg
	}
	switch u.hub.SlowConsumer {
	case "disconnect":
		select {
		case s.Out <- rewritten:
		default:
			s.Detach()
		}
	case "backpressure":
		// Block until the client drains. Affects all clients on this
		// upstream — operator opted in.
		select {
		case s.Out <- rewritten:
		case <-s.Done():
		}
	case "drop", "":
		// Drop the oldest queued event for this client only.
		for {
			select {
			case s.Out <- rewritten:
				return
			default:
				select {
				case <-s.Out:
					s.dropCount.Add(1)
				default:
				}
			}
		}
	}
}

// failAllClients closes every attached subscriber's done channel.
func (u *hubUpstream) failAllClients(reason string) {
	u.mu.Lock()
	clients := append([]*Subscriber(nil), u.clients...)
	u.mu.Unlock()
	for _, c := range clients {
		c.Detach()
	}
	_ = reason
}

// bytes_TrimSpace is a tiny duplicate to avoid an extra import in the
// hub file; the inj_session has the same helper privately. Keeping
// them separate avoids cross-file refactors during phase 5d.
func bytes_TrimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\n' || b[0] == '\r') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\t' || b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// injWSURLNormalize duplicates the small normalization helper used in
// inj_session so the hub doesn't import the session's internals.
func injWSURLNormalize(s string) string {
	switch {
	case len(s) > 5 && (s[:5] == "wss:/" || s[:5] == "ws://"):
		return s
	case len(s) > 7 && s[:7] == "https:/":
		return "wss://" + s[8:] + "/injstream-ws"
	case len(s) > 7 && s[:7] == "http://":
		return "ws://" + s[7:] + "/injstream-ws"
	}
	return "ws://" + s + "/injstream-ws"
}
