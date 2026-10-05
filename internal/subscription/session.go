package subscription

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/InjectiveLabs/stitch/internal/metrics"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
	"github.com/InjectiveLabs/stitch/internal/wsurl"
)

// Session is one eth_ws client WebSocket connection paired with one
// upstream connection. The session lives until either side closes for
// good (no resumable subscriptions) or until ctx is cancelled.
//
// Session is a thin wrapper: the shared engine (engine.go) owns the
// connection/goroutine mechanics; ethAdapter below owns the eth_subscribe
// protocol rules.
type Session struct {
	eng *engine
	ad  *ethAdapter
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

// NewSession constructs a session pinned to the given client connection.
// Run() drives it until the client or all upstreams give up.
func NewSession(client *websocket.Conn, cfg SessionConfig) *Session {
	ad := newEthAdapter()
	eng := newEngine(client, cfg.Selector, cfg.Dialer, cfg.HandshakeTimeout, ad)
	eng.replayTimeout = cfg.ReplayTimeout
	return &Session{eng: eng, ad: ad}
}

// Run blocks until the session terminates. Returns the terminal cause.
func (s *Session) Run(ctx context.Context) error {
	return s.eng.run(ctx, s.clientReader)
}

// clientReader delegates to the engine's pump. The indirection exists so
// the spawned goroutine's stack keeps a (*Session).clientReader frame —
// the lifecycle test probes goroutine dumps for that symbol.
func (s *Session) clientReader(clientCh chan<- []byte, errCh chan<- error) {
	s.eng.clientReader(clientCh, errCh)
}

// routeUpstreamFrame feeds one upstream frame through the adapter — the
// same seam the engine's upstreamReader drives; kept as a method so
// package tests can inject frames without a live upstream.
func (s *Session) routeUpstreamFrame(_ context.Context, msg []byte) error {
	return s.ad.HandleUpstreamFrame(s.eng, msg)
}

// Backend names the currently bound upstream — used by tests.
func (s *Session) Backend() string {
	return s.eng.backendName()
}

// ethAdapter implements the eth_ws protocol half of a session:
//
//   - eth_subscribe is intercepted; a synthetic subscription ID
//     ("0x%016x") is minted per sub and the upstream-minted ID is hidden
//     from the client, so upstream swaps don't change client-visible ids.
//   - Non-resumable kinds (newPendingTransactions, syncing) terminate the
//     session on upstream death rather than forging continuity.
//   - Notifications for unknown upstream ids are dropped (and counted) —
//     they belong to a dead epoch or an unsubscribed sub.
type ethAdapter struct {
	mu         sync.Mutex
	subs       map[string]*Sub            // synthetic ID → sub
	upToSyn    map[string]string          // upstream-minted ID → synthetic ID
	pending    map[string]*Sub            // our outgoing JSON-RPC id → pending sub awaiting response
	rpcPending map[string]json.RawMessage // outgoing id → client id; nil swallows a locally acknowledged unsubscribe
	synSeq     atomic.Uint64
	idSeq      atomic.Uint64
}

func newEthAdapter() *ethAdapter {
	return &ethAdapter{
		subs:       make(map[string]*Sub),
		upToSyn:    make(map[string]string),
		pending:    make(map[string]*Sub),
		rpcPending: make(map[string]json.RawMessage),
	}
}

func (a *ethAdapter) DialRouteKey() types.RouteKey {
	return types.RouteKey{
		Protocol: types.ProtoEthWS,
		Method:   "subscribe_session",
		Class:    types.ClassLatest,
	}
}

func (a *ethAdapter) NormalizeEndpoint(ep string) string { return wsurl.Normalize(ep) }

func (a *ethAdapter) SessionLabels() (string, string) { return string(types.ProtoEthWS), "session" }

func (a *ethAdapter) ResumeReason() string { return "upstream_close" }

// HandleClientFrame inspects an incoming client frame and either:
//   - rejects batches containing subscription methods before any forwarding
//   - intercepts an eth_subscribe (records pending) and forwards a
//     stitch-issued copy to upstream
//   - intercepts an eth_unsubscribe by synthetic ID and rewrites it
//   - or forwards with an internal numeric ID, restoring the client ID on reply
func (a *ethAdapter) HandleClientFrame(io sessionIO, msg []byte) error {
	if handled, err := rejectEthSubscriptionBatch(io, msg); handled {
		return err
	}
	var probe struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	_ = json.Unmarshal(msg, &probe)

	switch probe.Method {
	case "eth_subscribe":
		return a.handleClientSubscribe(io, probe.ID, probe.Params)
	case "eth_unsubscribe":
		return a.handleClientUnsubscribe(io, probe.ID, probe.Params)
	default:
		out, err := rewriteRPCIDs(msg, func(clientID json.RawMessage) (json.RawMessage, bool) {
			internalID := a.nextID()
			a.mu.Lock()
			a.rpcPending[internalID.String()] = append(json.RawMessage(nil), clientID...)
			a.mu.Unlock()
			return json.RawMessage(internalID.String()), true
		})
		if err != nil {
			return err
		}
		return io.upstreamWrite(out)
	}
}

// handleClientSubscribe registers a pending subscription with a fresh
// stitch JSON-RPC id, forwards to upstream. On the upstream response,
// we'll mint the synthetic ID and reply to the client.
func (a *ethAdapter) handleClientSubscribe(io sessionIO, clientID, params json.RawMessage) error {
	kind := readSubscribeKind(params)
	syn := a.mintSynthetic()
	internalID := a.nextID()

	sub := &Sub{
		SyntheticID: syn,
		Kind:        kind,
		Params:      append([]byte(nil), params...),
		ClientID:    append([]byte(nil), clientID...),
		Resumable:   kind.Resumable(),
	}

	a.mu.Lock()
	a.subs[syn] = sub
	a.pending[internalID.String()] = sub
	a.mu.Unlock()

	out, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      internalID,
		"method":  "eth_subscribe",
		"params":  json.RawMessage(params),
	})
	if err != nil {
		return err
	}
	return io.upstreamWrite(out)
}

// handleClientUnsubscribe maps the synthetic ID back to upstream, sends
// the upstream-form unsubscribe, replies "true" to the client, and forgets
// the sub. Unknown ids reply result=false.
func (a *ethAdapter) handleClientUnsubscribe(io sessionIO, clientID, params json.RawMessage) error {
	id := firstStringParam(params)
	a.mu.Lock()
	sub, ok := a.subs[id]
	var upID string
	if ok {
		delete(a.subs, id)
		if sub.UpstreamID != "" {
			delete(a.upToSyn, sub.UpstreamID)
		}
		upID = sub.UpstreamID
	}
	a.mu.Unlock()

	if !ok {
		return io.clientReplyBool(clientID, false)
	}
	if upID != "" {
		internalID := a.nextID()
		a.mu.Lock()
		a.rpcPending[internalID.String()] = nil
		a.mu.Unlock()
		out, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      internalID,
			"method":  "eth_unsubscribe",
			"params":  []string{upID},
		})
		_ = io.upstreamWrite(out)
	}
	return io.clientReplyBool(clientID, true)
}

// HandleUpstreamFrame inspects a frame from upstream:
//   - notification: translate id, dedup, forward
//   - response with our internal id: bind synthetic, reply to client
//   - ordinary response: restore the client ID (may be eth_call etc.)
func (a *ethAdapter) HandleUpstreamFrame(io sessionIO, msg []byte) error {
	var probe struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
		Result json.RawMessage `json:"result"`
	}
	_ = json.Unmarshal(msg, &probe)

	if probe.Method == "eth_subscription" {
		return a.handleUpstreamNotification(io, msg)
	}
	if len(probe.ID) > 0 {
		idStr := unquoteID(probe.ID)
		a.mu.Lock()
		pending, ok := a.pending[idStr]
		if ok {
			delete(a.pending, idStr)
		}
		a.mu.Unlock()
		if ok {
			return a.handleUpstreamSubscribeResp(io, pending, probe.Result)
		}
	}
	out, err := rewriteRPCIDs(msg, func(internalID json.RawMessage) (json.RawMessage, bool) {
		a.mu.Lock()
		clientID, ok := a.rpcPending[unquoteID(internalID)]
		if ok {
			delete(a.rpcPending, unquoteID(internalID))
		}
		a.mu.Unlock()
		if !ok {
			return internalID, true
		}
		return clientID, clientID != nil
	})
	if err != nil || len(out) == 0 {
		return err
	}
	return io.clientWrite(out)
}

func (a *ethAdapter) handleUpstreamNotification(io sessionIO, msg []byte) error {
	// Parse to find the upstream sub ID.
	var env struct {
		Params struct {
			Subscription string `json:"subscription"`
		} `json:"params"`
	}
	if err := json.Unmarshal(msg, &env); err != nil {
		return io.clientWrite(msg)
	}
	upID := env.Params.Subscription

	a.mu.Lock()
	syn, ok := a.upToSyn[upID]
	var sub *Sub
	if ok {
		sub = a.subs[syn]
	}
	a.mu.Unlock()
	if !ok || sub == nil {
		metrics.SubscriptionDroppedNotifs.WithLabelValues(string(types.ProtoEthWS), "unknown_sub").Inc()
		return nil // unknown sub — drop
	}

	// Dedup against cursor. Equal-or-behind events are resume duplicates,
	// EXCEPT when the sub's cursor is still zero (first notification ever
	// must pass even if its own cursor parses as zero).
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
		a.mu.Lock()
		sub.Cursor = parsed.Cursor
		a.mu.Unlock()
	}
	return io.clientWrite(rewritten)
}

func (a *ethAdapter) handleUpstreamSubscribeResp(io sessionIO, sub *Sub, result json.RawMessage) error {
	var upID string
	if err := json.Unmarshal(result, &upID); err != nil {
		// Subscribe failed — propagate error to client.
		return io.clientReplyError(sub.ClientID, -32603, "upstream subscribe failed")
	}
	a.mu.Lock()
	sub.UpstreamID = upID
	a.upToSyn[upID] = sub.SyntheticID
	clientID := append(json.RawMessage(nil), sub.ClientID...)
	syn := sub.SyntheticID
	first := sub.Cursor.IsZero() // first-time subscribe (not a re-issue)
	a.mu.Unlock()

	if !first {
		return nil // resume: don't re-reply; the original response already went out
	}
	return io.clientReplyResult(clientID, syn)
}

// ReplaySubs is called after a (re)connect. For each resumable sub,
// re-issue eth_subscribe with the original params; the upstream's
// response binds a fresh upstream id. The stale upstream-id mapping is
// purged before re-issue so a late notification from the dead upstream
// can't sneak through. Aborts on the first write error and returns it.
func (a *ethAdapter) ReplaySubs(_ context.Context, io sessionIO) error {
	a.mu.Lock()
	// The engine has joined the old upstream reader before entering this
	// epoch. Ordinary calls are not replayed, and their replies can no longer
	// arrive, so discard their correlation entries (including unsubscribe acks).
	clear(a.rpcPending)
	subs := make([]*Sub, 0, len(a.subs))
	for _, sub := range a.subs {
		if sub.Resumable {
			if sub.UpstreamID != "" {
				delete(a.upToSyn, sub.UpstreamID)
				sub.UpstreamID = ""
			}
			subs = append(subs, sub)
		}
	}
	a.mu.Unlock()

	for _, sub := range subs {
		internalID := a.nextID()
		a.mu.Lock()
		// Stale pending entries from never-acked epochs are deliberately
		// retained: they serve the late-notification fallback window, ids
		// never collide, and the cost is memory-only, bounded by flap count.
		a.pending[internalID.String()] = sub
		a.mu.Unlock()
		out, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      internalID,
			"method":  "eth_subscribe",
			"params":  json.RawMessage(sub.Params),
		})
		if err := io.upstreamWrite(out); err != nil {
			return err
		}
	}
	return nil
}

// ResumableSubs counts subs that survive a backend swap. Zero means the
// engine terminates the session instead of reconnecting.
func (a *ethAdapter) ResumableSubs() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, sub := range a.subs {
		if sub.Resumable {
			n++
		}
	}
	return n
}

func (a *ethAdapter) mintSynthetic() string {
	return fmt.Sprintf("0x%016x", a.synSeq.Add(1))
}

func (a *ethAdapter) nextID() json.Number {
	// Injective's EVM WebSocket server requires numeric request IDs. Keep our
	// sequence separate from client IDs, including ordinary calls, so a client
	// request cannot collide with a pending subscribe or replay response.
	return json.Number(strconv.FormatUint(a.idSeq.Add(1), 10))
}

// rewriteRPCIDs preserves raw IDs (including numbers above 2^53) and batch
// response shapes without decoding any JSON number through float64. Notifications
// and malformed frames are left untouched. keep=false drops an internal reply.
func rewriteRPCIDs(msg []byte, rewrite func(json.RawMessage) (json.RawMessage, bool)) ([]byte, error) {
	trimmed := bytes.TrimSpace(msg)
	if len(trimmed) == 0 {
		return msg, nil
	}
	if trimmed[0] == '[' {
		var batch []json.RawMessage
		if err := json.Unmarshal(msg, &batch); err != nil || len(batch) == 0 {
			return msg, nil
		}
		out := make([]json.RawMessage, 0, len(batch))
		for _, item := range batch {
			rewritten, err := rewriteRPCIDs(item, rewrite)
			if err != nil {
				return nil, err
			}
			if len(rewritten) > 0 {
				out = append(out, rewritten)
			}
		}
		if len(out) == 0 {
			return nil, nil
		}
		return json.Marshal(out)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(msg, &envelope); err != nil {
		return msg, nil
	}
	id, exists := envelope["id"]
	if !exists {
		return msg, nil
	}
	replacement, keep := rewrite(id)
	if !keep {
		return nil, nil
	}
	envelope["id"] = replacement
	return json.Marshal(envelope)
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
