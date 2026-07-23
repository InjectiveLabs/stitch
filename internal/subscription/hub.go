package subscription

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/InjectiveLabs/stitch/internal/log"
	"github.com/InjectiveLabs/stitch/internal/metrics"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
	"github.com/InjectiveLabs/stitch/internal/wsurl"
)

// ErrHubClosed is returned by Subscribe once Shutdown has begun.
var ErrHubClosed = errors.New("subscription hub: shut down")

// Hub multiplexes /injstream-ws upstreams. Multiple client sessions that
// subscribe to filters with the same canonical key share one upstream
// connection; the hub fans events out to each client's send channel.
//
// On upstream death the hub re-dials, replays the subscribe, and dedups
// any events at-or-behind the shared cursor. Clients see no
// interruption beyond the millisecond-level reconnect window.
//
// The hub owns its upstream goroutines: an upstream exits promptly when
// its last subscriber detaches, and Shutdown tears down every upstream
// regardless of attached subscribers. SlowConsumer, SendBufSize, and
// ReplayTimeout must be set before the first Subscribe.
//
// Scope: /injstream-ws JSON-RPC over WS only. The same shape will work
// for eth_ws (JSON canonicalization) and for chainstream gRPC (proto
// canonicalization) once those adapters land.
type Hub struct {
	selector selector.Selector
	dialer   *websocket.Dialer

	// ctx is the hub lifecycle: every upstream derives from it, and
	// Shutdown cancels it. wg counts live upstream run goroutines.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// tuning carries the upstream dial/read-deadline/keepalive knobs;
	// always defaultConnTuning in production, tightened by package tests.
	tuning connTuning

	mu        sync.Mutex
	closed    bool
	upstreams map[string]*hubUpstream

	// SlowConsumer is the policy applied when a subscriber's Out channel
	// is full. "drop" (default) drops the oldest queued event for that
	// client only. "disconnect" closes the subscriber. "backpressure"
	// blocks the upstream — only useful for log-indexer style consumers.
	SlowConsumer string

	// SendBufSize sets the per-subscriber send-channel capacity.
	SendBufSize int

	// ReplayTimeout is the max time an upstream keeps retrying dial passes
	// during resume before giving up and detaching its subscribers.
	// <= 0 means a single dial pass per resume.
	ReplayTimeout time.Duration
}

func NewHub(sel selector.Selector, dialer *websocket.Dialer) *Hub {
	if dialer == nil {
		dialer = &websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Hub{
		selector:     sel,
		dialer:       dialer,
		ctx:          ctx,
		cancel:       cancel,
		tuning:       defaultConnTuning(),
		upstreams:    make(map[string]*hubUpstream),
		SlowConsumer: "drop",
		SendBufSize:  64,
	}
}

// Subscribe attaches a new client to the upstream for filter's canonical
// key. Caller-supplied subscription ID and synthetic ID are stored on
// the Subscriber and used to rewrite outgoing notifications. ctx is
// accepted for API symmetry only — upstream lifecycle is hub-scoped, not
// call-scoped.
//
// On error, the caller should report the error to its client; on
// success, the caller sends the client its subscribe ack (the hub
// absorbs upstream acks) and spawns a goroutine that pumps
// Subscriber.Out into the client connection until Subscriber.Done()
// fires.
func (h *Hub) Subscribe(_ context.Context, filter json.RawMessage, clientSubscriptionID string, clientJSONRPCID json.RawMessage) (*Subscriber, error) {
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
	if h.closed {
		h.mu.Unlock()
		return nil, ErrHubClosed
	}
	// sub.upstream is assigned BEFORE attach publishes the subscriber to
	// the fan-out: the run goroutine may Detach it (slow-consumer
	// disconnect) the moment it becomes visible.
	up, ok := h.upstreams[key]
	if ok {
		sub.upstream = up
		// A map hit can still be a dying upstream (last client just
		// detached, run goroutine mid-teardown); attach refuses those and
		// we replace below.
		if !up.attach(sub) {
			ok = false
		}
	}
	if !ok {
		up = newHubUpstream(h, key, filter)
		sub.upstream = up
		up.attach(sub) // fresh upstream, attach cannot fail
		h.upstreams[key] = up
		h.wg.Add(1)
		go up.run()
	}
	h.mu.Unlock()

	metrics.SubscriptionsActive.WithLabelValues("hub", "client").Inc()
	return sub, nil
}

// Shutdown tears down every upstream — cancelling their lifecycle ctx
// closes the blocked conns — and waits, bounded by ctx, for the run
// goroutines to exit. Attached subscribers are detached on the way out
// (their Done fires). Subscribe fails with ErrHubClosed afterwards.
func (h *Hub) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	h.cancel()

	done := make(chan struct{})
	go func() { h.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// removeUpstream is called by a hubUpstream as it exits. The pointer
// guard keeps a dying upstream from deleting its replacement: Subscribe
// may have already swapped a fresh upstream in under the same key.
func (h *Hub) removeUpstream(key string, u *hubUpstream) {
	h.mu.Lock()
	if h.upstreams[key] == u {
		delete(h.upstreams, key)
	}
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
// subscribers. Its ctx derives from the hub's and is cancelled when the
// last subscriber detaches (or the hub shuts down); a per-conn watcher
// turns that cancel into a conn close, so a run loop blocked in
// ReadMessage exits promptly instead of at the read deadline.
type hubUpstream struct {
	hub    *Hub
	key    string
	filter json.RawMessage
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	cursor  int64
	conn    *websocket.Conn
	clients []*Subscriber
	// pending is drained on ack AND on error-reject; the remaining leak
	// case — epochs that are never answered at all — is bounded by the
	// resume count and left alone deliberately.
	pending    map[string]struct{} // outgoing subscribe ids awaiting ack
	upstreamID string              // current id used in subscribe to upstream
	idSeq      atomic.Uint64
	closed     bool
}

func newHubUpstream(h *Hub, key string, filter json.RawMessage) *hubUpstream {
	ctx, cancel := context.WithCancel(h.ctx)
	return &hubUpstream{
		hub:     h,
		key:     key,
		filter:  append(json.RawMessage(nil), filter...),
		ctx:     ctx,
		cancel:  cancel,
		pending: make(map[string]struct{}),
	}
}

// attach adds a subscriber. It refuses — returns false — once the
// upstream is closing or cancelled, so Subscribe replaces the entry
// instead of binding a client to a goroutine that is about to exit.
// One sliver remains: a run loop exiting via dial-failure sets closed
// only in its defer (ctx not yet cancelled), so an attach can land on it
// — that client gets its ack and then the prompt 1013 close when
// failAllClients detaches it; safe, bounded, deliberate.
func (u *hubUpstream) attach(s *Subscriber) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed || u.ctx.Err() != nil {
		return false
	}
	u.clients = append(u.clients, s)
	return true
}

// detach removes a subscriber; the last one out cancels the upstream's
// ctx. The cancel happens under u.mu so a racing attach can't slip in
// between the emptiness check and the cancel — attach re-checks ctx
// under the same lock.
func (u *hubUpstream) detach(s *Subscriber) {
	u.mu.Lock()
	for i, c := range u.clients {
		if c == s {
			u.clients = append(u.clients[:i], u.clients[i+1:]...)
			break
		}
	}
	if len(u.clients) == 0 {
		u.cancel()
	}
	u.mu.Unlock()
}

// run is the upstream's lifecycle goroutine. It dials, sends the
// subscribe, fans out notifications until every client has detached,
// the hub shuts down, or the upstream is unrecoverable. On the way out
// it detaches any remaining subscribers (their Done fires).
func (u *hubUpstream) run() {
	defer func() {
		u.mu.Lock()
		u.closed = true
		conn := u.conn
		u.conn = nil
		u.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		u.failAllClients()
		u.hub.removeUpstream(u.key, u)
		u.cancel() // release the ctx even on the no-detach exit paths
		u.hub.wg.Done()
		log.L().Debug("hub upstream closed", "key", u.key)
	}()

	for resume := false; ; resume = true {
		if u.ctx.Err() != nil {
			return
		}
		if !u.dialLoop(resume) {
			return
		}
		err := u.runUntilFailure()
		if err == nil || u.ctx.Err() != nil {
			return
		}
		log.L().Info("hub: upstream lost, resuming", "key", u.key, "err", err.Error())
		metrics.SubscriptionResumes.WithLabelValues("hub_upstream_close").Inc()
	}
}

// dialLoop makes one dial pass; on resume it keeps making passes —
// doubling backoff between them — until the hub's ReplayTimeout window
// closes. The initial dial stays single-pass: there is nothing to
// resume yet, so subscribers learn about a dead fleet immediately.
func (u *hubUpstream) dialLoop(resume bool) bool {
	deadline := time.Now().Add(u.hub.ReplayTimeout)
	if u.dial() {
		return true
	}
	if !resume || u.hub.ReplayTimeout <= 0 {
		return false
	}
	backoff := resumeBackoffMin
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		select {
		case <-u.ctx.Done():
			return false
		case <-time.After(min(backoff, remaining)):
		}
		if u.dial() {
			return true
		}
		backoff = min(backoff*2, resumeBackoffMax)
	}
}

// dial makes one pass over the selector candidates and binds the first
// /injstream-ws endpoint that answers. Returns false if nothing is
// dialable.
func (u *hubUpstream) dial() bool {
	conn, backend, ok := dialFirstCandidate(u.ctx, u.hub.selector, u.hub.dialer, types.RouteKey{
		Protocol: types.ProtoChainStream,
		Method:   "hub_inj_ws",
		Class:    types.ClassLatest,
	}, wsurl.InjStreamURL, "hub", u.hub.tuning)
	if !ok {
		return false
	}
	u.mu.Lock()
	u.conn = conn
	u.mu.Unlock()
	log.L().Info("hub: upstream connected", "key", u.key, "backend", backend)
	return true
}

// runUntilFailure issues subscribe and fans out notifications.
// Returns nil if the upstream finished cleanly (ctx cancelled: last
// client gone or hub shutdown) or an error if the connection died.
func (u *hubUpstream) runUntilFailure() error {
	u.mu.Lock()
	conn := u.conn
	u.mu.Unlock()
	if conn == nil {
		return errors.New("no upstream conn")
	}

	// Interrupt the blocked read when the upstream loses its reason to
	// live: the last detach (or hub shutdown) cancels u.ctx and the
	// watcher closes the conn, so ReadMessage returns now rather than at
	// the read deadline. Same shape as health/probe_eth_ws.go's
	// stop-channel goroutine.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-u.ctx.Done():
			_ = conn.Close()
		case <-stop:
		}
	}()
	go keepAliveLoop(conn, u.hub.tuning, stop)

	if err := u.issueSubscribe(); err != nil {
		return err
	}

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if u.ctx.Err() != nil {
				return nil // deliberate teardown, not an upstream failure
			}
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(u.hub.tuning.readDeadline)) // liveness = data OR pong (see connTuning)
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
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(msg, &probe); err != nil {
		return
	}
	// Upstream rejected the hub's own subscribe: an error-shaped response
	// whose id matches a pending entry. Drain pending, count it, and END
	// the upstream — cancelling u.ctx makes run exit, whose defer detaches
	// every subscriber, so client pumps see Done-while-owned and close
	// their sessions with 1013 promptly instead of leaving clients parked
	// forever on a dead subscription that acked "success" at attach time.
	// Deliberately NOT a resume/replay: replaying the same rejected filter
	// would be rejected again, forever.
	if errBody := bytes.TrimSpace(probe.Error); len(errBody) > 0 && !bytes.Equal(errBody, []byte("null")) {
		idStr := unquoteID(probe.ID)
		u.mu.Lock()
		_, wasPending := u.pending[idStr]
		delete(u.pending, idStr)
		u.mu.Unlock()
		if wasPending {
			log.L().Warn("hub: upstream rejected subscribe; ending shared upstream",
				"key", u.key, "error", string(errBody))
			metrics.SubscriptionDroppedNotifs.WithLabelValues(string(types.ProtoChainStream), "upstream_reject").Inc()
			u.cancel()
		}
		return
	}
	res := bytes.TrimSpace(probe.Result)
	// Subscribe ack: result is a string ("success"). Drain pending; do
	// NOT forward (the per-client ack was already sent at attach time —
	// see HubSession.handleSubscribe).
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
// consumer policy when the channel is full. Notifications lost to the
// policy — evicted-oldest under "drop", the triggering event under
// "disconnect" — are counted in SubscriptionDroppedNotifs.
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
			metrics.SubscriptionDroppedNotifs.WithLabelValues(string(types.ProtoChainStream), "slow_consumer").Inc()
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
					metrics.SubscriptionDroppedNotifs.WithLabelValues(string(types.ProtoChainStream), "slow_consumer").Inc()
				default:
				}
			}
		}
	}
}

// failAllClients detaches every remaining subscriber (Done fires).
// No-op on the clean path where the last client already detached.
func (u *hubUpstream) failAllClients() {
	u.mu.Lock()
	clients := append([]*Subscriber(nil), u.clients...)
	u.mu.Unlock()
	for _, c := range clients {
		c.Detach()
	}
}
