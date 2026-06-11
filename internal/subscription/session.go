package subscription

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/types"
	"github.com/decentrio/stitch/internal/wsurl"
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
	Selector         selector.Selector
	Dialer           *websocket.Dialer
	HandshakeTimeout time.Duration
}

// NewSession constructs a session pinned to the given client connection.
// Run() drives it until the client or all upstreams give up.
func NewSession(client *websocket.Conn, cfg SessionConfig) *Session {
	ad := newEthAdapter()
	return &Session{
		eng: newEngine(client, cfg.Selector, cfg.Dialer, ad),
		ad:  ad,
	}
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
	mu      sync.Mutex
	subs    map[string]*Sub   // synthetic ID → sub
	upToSyn map[string]string // upstream-minted ID → synthetic ID
	pending map[string]*Sub   // our outgoing JSON-RPC id → pending sub awaiting response
	synSeq  atomic.Uint64
	idSeq   atomic.Uint64
}

func newEthAdapter() *ethAdapter {
	return &ethAdapter{
		subs:    make(map[string]*Sub),
		upToSyn: make(map[string]string),
		pending: make(map[string]*Sub),
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
//   - intercepts an eth_subscribe (records pending) and forwards a
//     stitch-issued copy to upstream
//   - intercepts an eth_unsubscribe by synthetic ID and rewrites it
//   - or forwards verbatim
func (a *ethAdapter) HandleClientFrame(e *engine, msg []byte) error {
	var probe struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	_ = json.Unmarshal(msg, &probe)

	switch probe.Method {
	case "eth_subscribe":
		return a.handleClientSubscribe(e, probe.ID, probe.Params)
	case "eth_unsubscribe":
		return a.handleClientUnsubscribe(e, probe.ID, probe.Params)
	default:
		return e.upstreamWrite(msg)
	}
}

// handleClientSubscribe registers a pending subscription with a fresh
// stitch JSON-RPC id, forwards to upstream. On the upstream response,
// we'll mint the synthetic ID and reply to the client.
func (a *ethAdapter) handleClientSubscribe(e *engine, clientID, params json.RawMessage) error {
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
	a.pending[internalID] = sub
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
	return e.upstreamWrite(out)
}

// handleClientUnsubscribe maps the synthetic ID back to upstream, sends
// the upstream-form unsubscribe, replies "true" to the client, and forgets
// the sub. Unknown ids reply result=false.
func (a *ethAdapter) handleClientUnsubscribe(e *engine, clientID, params json.RawMessage) error {
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
		return e.clientReplyBool(clientID, false)
	}
	if upID != "" {
		out, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      a.nextID(),
			"method":  "eth_unsubscribe",
			"params":  []string{upID},
		})
		_ = e.upstreamWrite(out)
	}
	return e.clientReplyBool(clientID, true)
}

// HandleUpstreamFrame inspects a frame from upstream:
//   - notification: translate id, dedup, forward
//   - response with our internal id: bind synthetic, reply to client
//   - other response: forward verbatim (may be eth_call etc.)
func (a *ethAdapter) HandleUpstreamFrame(e *engine, msg []byte) error {
	var probe struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
		Result json.RawMessage `json:"result"`
	}
	_ = json.Unmarshal(msg, &probe)

	if probe.Method == "eth_subscription" {
		return a.handleUpstreamNotification(e, msg)
	}
	if len(probe.ID) > 0 {
		idStr := strings.Trim(string(probe.ID), `"`)
		a.mu.Lock()
		pending, ok := a.pending[idStr]
		if ok {
			delete(a.pending, idStr)
		}
		a.mu.Unlock()
		if ok {
			return a.handleUpstreamSubscribeResp(e, pending, probe.Result)
		}
	}
	return e.clientWrite(msg)
}

func (a *ethAdapter) handleUpstreamNotification(e *engine, msg []byte) error {
	// Parse to find the upstream sub ID.
	var env struct {
		Params struct {
			Subscription string `json:"subscription"`
		} `json:"params"`
	}
	if err := json.Unmarshal(msg, &env); err != nil {
		return e.clientWrite(msg)
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
	return e.clientWrite(rewritten)
}

func (a *ethAdapter) handleUpstreamSubscribeResp(e *engine, sub *Sub, result json.RawMessage) error {
	var upID string
	if err := json.Unmarshal(result, &upID); err != nil {
		// Subscribe failed — propagate error to client.
		return e.clientReplyError(sub.ClientID, -32603, "upstream subscribe failed")
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
	return e.clientReplyResult(clientID, syn)
}

// ReplaySubs is called after a (re)connect. For each resumable sub,
// re-issue eth_subscribe with the original params; the upstream's
// response binds a fresh upstream id. The stale upstream-id mapping is
// purged before re-issue so a late notification from the dead upstream
// can't sneak through.
func (a *ethAdapter) ReplaySubs(_ context.Context, e *engine) {
	a.mu.Lock()
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
		a.pending[internalID] = sub
		a.mu.Unlock()
		out, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      internalID,
			"method":  "eth_subscribe",
			"params":  json.RawMessage(sub.Params),
		})
		if err := e.upstreamWrite(out); err != nil {
			return
		}
	}
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

func (a *ethAdapter) nextID() string {
	return fmt.Sprintf("stitch_%d", a.idSeq.Add(1))
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
