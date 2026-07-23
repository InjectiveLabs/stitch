package subscription

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/InjectiveLabs/stitch/internal/metrics"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
	"github.com/InjectiveLabs/stitch/internal/wsurl"
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
//
// InjSession is a thin wrapper: the shared engine (engine.go) owns the
// connection/goroutine mechanics; injAdapter below owns the protocol.
type InjSession struct {
	eng *engine
	ad  *injAdapter
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
	Selector selector.Selector
	Dialer   *websocket.Dialer
	// HandshakeTimeout bounds the default upstream dialer's WS handshake;
	// used only when Dialer is nil.
	HandshakeTimeout time.Duration
	// ReplayTimeout is the max time to wait for a dialable upstream during
	// resume before terminating the session (policies.subscriptions.
	// replay_timeout). <= 0 means a single dial pass per resume.
	ReplayTimeout time.Duration
}

// NewInjSession constructs a /injstream-ws session.
func NewInjSession(client *websocket.Conn, cfg InjSessionConfig) *InjSession {
	ad := newInjAdapter()
	eng := newEngine(client, cfg.Selector, cfg.Dialer, cfg.HandshakeTimeout, ad)
	eng.replayTimeout = cfg.ReplayTimeout
	return &InjSession{eng: eng, ad: ad}
}

// Run blocks until terminated. Returns the terminal cause.
func (s *InjSession) Run(ctx context.Context) error {
	return s.eng.run(ctx, s.clientReader)
}

// clientReader delegates to the engine's pump. The indirection exists so
// the spawned goroutine's stack keeps an (*InjSession).clientReader frame
// — the lifecycle test probes goroutine dumps for that symbol.
func (s *InjSession) clientReader(clientCh chan<- []byte, errCh chan<- error) {
	s.eng.clientReader(clientCh, errCh)
}

// routeUpstreamFrame feeds one upstream frame through the adapter — the
// same seam the engine's upstreamReader drives; kept as a method so
// package tests can inject frames without a live upstream.
func (s *InjSession) routeUpstreamFrame(_ context.Context, msg []byte) error {
	return s.ad.HandleUpstreamFrame(s.eng, msg)
}

// injAdapter implements the /injstream-ws protocol half of a session:
//
//   - The client-supplied subscription_id keys the subscription; the
//     internal id ("stitch_inj_%d") is only the upstream correlation and
//     is re-minted on every resume.
//   - The subscribe ack is the literal result string "success". The
//     first-time ack is forwarded under the client's original JSON-RPC
//     id; resume acks (cursor already advanced) are absorbed.
//   - Unparseable client frames are forwarded verbatim upstream, and
//     unknown upstream frames verbatim to the client (the eth adapter
//     drops unknown-sub notifications instead — /injstream-ws responses
//     have no method field to discriminate on, so passthrough is the
//     protocol-correct default for unrecognized shapes).
type injAdapter struct {
	mu      sync.Mutex
	subs    map[string]*InjSub // subscription_id → sub
	pending map[string]*InjSub // our internal JSON-RPC id → sub awaiting upstream ack
	// byUpID indexes each live sub under its CURRENT UpstreamID — a mirror
	// of subs (same values, different key) maintained at subscribe,
	// unsubscribe, and replay so notification matching is a lookup, not a
	// scan. Entries for replaced/removed subs are deleted eagerly so
	// stale-id notifications can't match a dead sub.
	byUpID map[string]*InjSub
	idSeq  atomic.Uint64
}

func newInjAdapter() *injAdapter {
	return &injAdapter{
		subs:    make(map[string]*InjSub),
		pending: make(map[string]*InjSub),
		byUpID:  make(map[string]*InjSub),
	}
}

// DialRouteKey reuses ProtoChainStream — a chainstream-capable backend is
// assumed to expose /injstream-ws on the same host but a different port;
// operators put the WS URL in the `chainstream` endpoint slot when they
// want it routable by stitch.
func (a *injAdapter) DialRouteKey() types.RouteKey {
	return types.RouteKey{
		Protocol: types.ProtoChainStream,
		Method:   "inj_ws_session",
		Class:    types.ClassLatest,
	}
}

func (a *injAdapter) NormalizeEndpoint(ep string) string { return wsurl.InjStreamURL(ep) }

func (a *injAdapter) SessionLabels() (string, string) {
	return string(types.ProtoChainStream), "inj_ws"
}

func (a *injAdapter) ResumeReason() string { return "inj_ws_upstream_close" }

// HandleClientFrame intercepts subscribe/unsubscribe; everything else —
// including frames we cannot parse — is forwarded verbatim so upstream
// produces the protocol's own error responses.
func (a *injAdapter) HandleClientFrame(io sessionIO, msg []byte) error {
	var probe struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(msg, &probe); err != nil {
		// Forward verbatim if we can't parse; let upstream handle errors.
		return io.upstreamWrite(msg)
	}
	switch probe.Method {
	case "subscribe":
		return a.handleSubscribe(io, probe.ID, probe.Params)
	case "unsubscribe":
		return a.handleUnsubscribe(io, probe.ID, probe.Params)
	default:
		return io.upstreamWrite(msg)
	}
}

// handleSubscribe registers the sub locally, reissues to upstream with
// our internal JSON-RPC id, and awaits the ack.
func (a *injAdapter) handleSubscribe(io sessionIO, clientID, params json.RawMessage) error {
	sp, ok := ParseInjSubscribeParams(params)
	if !ok {
		return io.clientReplyError(clientID, -32602, "subscribe params: missing subscription_id or filter")
	}
	internalID := a.nextID()
	sub := &InjSub{
		SubscriptionID:  sp.SubscriptionID,
		ClientJSONRPCID: append(json.RawMessage(nil), clientID...),
		Filter:          append(json.RawMessage(nil), sp.Filter...),
		UpstreamID:      internalID,
	}
	a.mu.Lock()
	if old, exists := a.subs[sp.SubscriptionID]; exists {
		// Re-subscribe under the same id replaces the sub; drop the stale
		// correlation so old-id notifications can't match the dead one.
		delete(a.byUpID, old.UpstreamID)
	}
	a.subs[sp.SubscriptionID] = sub
	a.pending[internalID] = sub
	a.byUpID[internalID] = sub
	a.mu.Unlock()

	out, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      internalID,
		"method":  "subscribe",
		"params":  map[string]any{"subscription_id": sp.SubscriptionID, "filter": json.RawMessage(sp.Filter)},
	})
	if err != nil {
		return err
	}
	return io.upstreamWrite(out)
}

// handleUnsubscribe forgets the sub, forwards the unsubscribe upstream,
// and always replies "success" to the client afterwards.
func (a *injAdapter) handleUnsubscribe(io sessionIO, clientID, params json.RawMessage) error {
	up, ok := ParseInjUnsubscribeParams(params)
	if !ok {
		return io.clientReplyError(clientID, -32602, "unsubscribe params: missing subscription_id")
	}
	a.mu.Lock()
	if sub, exists := a.subs[up.SubscriptionID]; exists {
		delete(a.subs, up.SubscriptionID)
		delete(a.byUpID, sub.UpstreamID)
	}
	a.mu.Unlock()
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      a.nextID(),
		"method":  "unsubscribe",
		"params":  map[string]any{"subscription_id": up.SubscriptionID},
	})
	if err := io.upstreamWrite(out); err != nil {
		return err
	}
	// Reply success to client.
	return io.clientReplyResult(clientID, "success")
}

// HandleUpstreamFrame classifies a frame from upstream:
//   - result is a string → subscribe ack: resolve pending; forward the
//     first-time ack under the client's id, absorb resume acks
//   - result is an object with block_height → notification: match by
//     internal id, dedup against the cursor, rewrite the id, forward
//   - anything else → forward verbatim
func (a *injAdapter) HandleUpstreamFrame(io sessionIO, msg []byte) error {
	var probe struct {
		ID     json.RawMessage `json:"id"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(msg, &probe); err != nil {
		return io.clientWrite(msg)
	}
	resTrim := bytes.TrimSpace(probe.Result)

	// Subscribe ack: result is the literal string "success".
	if len(resTrim) > 0 && resTrim[0] == '"' {
		idStr := unquoteID(probe.ID)
		a.mu.Lock()
		sub, ok := a.pending[idStr]
		if ok {
			delete(a.pending, idStr)
		}
		a.mu.Unlock()
		if !ok {
			// Unknown id — pass through.
			return io.clientWrite(msg)
		}
		// First-time subscribe: forward ack with the client's original id.
		// Re-issue (resume): silently absorb.
		if !sub.Cursor.IsZero() {
			return nil
		}
		return io.clientReplyResult(sub.ClientJSONRPCID, "success")
	}

	// Notification: result is an object with block_height.
	notif, ok := ParseInjNotification(msg)
	if !ok {
		// Unknown frame shape — forward verbatim.
		return io.clientWrite(msg)
	}
	idStr := unquoteID(notif.ID)

	a.mu.Lock()
	// Notifications carry whatever id we used to subscribe; byUpID tracks
	// each sub under its current outgoing id (mutated on resume). Fall
	// back to the pending map for the brief window between subscribe
	// write and ack drain.
	sub := a.byUpID[idStr]
	if sub == nil {
		sub = a.pending[idStr]
	}
	a.mu.Unlock()
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
		a.mu.Lock()
		if notif.Cursor.LessEq(sub.Cursor) {
			a.mu.Unlock()
			return nil
		}
		sub.Cursor = notif.Cursor
		a.mu.Unlock()
	}
	return io.clientWrite(rewritten)
}

// ReplaySubs re-issues every active subscription on the freshly-dialed
// upstream. The upstream replies "success" (which HandleUpstreamFrame
// silently absorbs because Cursor is non-zero). Subsequent notifications
// match via the fresh internal id bound below. Aborts on the first write
// error and returns it.
func (a *injAdapter) ReplaySubs(_ context.Context, io sessionIO) error {
	a.mu.Lock()
	subs := make([]*InjSub, 0, len(a.subs))
	for _, sub := range a.subs {
		subs = append(subs, sub)
	}
	a.mu.Unlock()

	for _, sub := range subs {
		internalID := a.nextID()
		a.mu.Lock()
		// Forget the previous upstream id; new id replaces it.
		delete(a.byUpID, sub.UpstreamID)
		sub.UpstreamID = internalID
		// Stale pending entries from never-acked epochs are deliberately
		// retained: they serve the late-notification fallback window, ids
		// never collide, and the cost is memory-only, bounded by flap count.
		a.pending[internalID] = sub
		a.byUpID[internalID] = sub
		a.mu.Unlock()
		out, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      internalID,
			"method":  "subscribe",
			"params":  map[string]any{"subscription_id": sub.SubscriptionID, "filter": json.RawMessage(sub.Filter)},
		})
		if err := io.upstreamWrite(out); err != nil {
			return err
		}
	}
	return nil
}

// ResumableSubs reports the live sub count — every /injstream-ws sub is
// resumable (cursor dedup handles the overlap), so the engine reconnects
// whenever any sub is active.
func (a *injAdapter) ResumableSubs() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.subs)
}

func (a *injAdapter) nextID() string {
	return fmt.Sprintf("stitch_inj_%d", a.idSeq.Add(1))
}
