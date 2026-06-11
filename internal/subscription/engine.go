package subscription

import (
	"context"
	"encoding/json"
	"errors"
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

// protocolAdapter is the protocol-specific half of a proxied WebSocket
// subscription session. The engine owns every shared mechanic — dialing,
// goroutines, channels, teardown, serialized writes, the resume loop —
// and the adapter owns every protocol rule: method names, id minting and
// correlation, ack shapes, notification parsing, cursor semantics, and
// resumability. If a change touches a conn, a goroutine, or a lock on
// engine state, it belongs in the engine; if it touches JSON shapes or
// subscription bookkeeping, it belongs in the adapter.
//
// Adapters are stateful (they own the subscription tables and the mutex
// guarding them) and are constructed 1:1 with an engine. Adapter methods
// are invoked from exactly two goroutines: the run loop (HandleClientFrame,
// ReplaySubs, ResumableSubs) and the upstream reader (HandleUpstreamFrame).
type protocolAdapter interface {
	// DialRouteKey is the selector key for picking upstream candidates.
	// Its Protocol field also selects the endpoint slot on each backend.
	DialRouteKey() types.RouteKey

	// NormalizeEndpoint maps a configured endpoint to a dialable ws(s) URL.
	NormalizeEndpoint(ep string) string

	// SessionLabels returns the (protocol, kind) labels for the
	// SubscriptionsActive gauge. kind doubles as the engine's log tag.
	SessionLabels() (proto, kind string)

	// ResumeReason labels the SubscriptionResumes counter when the engine
	// decides to re-dial after upstream death.
	ResumeReason() string

	// HandleClientFrame consumes one client→upstream frame: intercept
	// subscribe/unsubscribe, otherwise forward via io.upstreamWrite. A
	// returned error terminates the session.
	HandleClientFrame(io sessionIO, msg []byte) error

	// HandleUpstreamFrame consumes one upstream→client frame: resolve
	// pending subscribe acks, dedup + rewrite notifications against
	// per-sub cursors, pass everything else through per protocol rules.
	// A returned error tears down the upstream (resume rules apply).
	HandleUpstreamFrame(io sessionIO, msg []byte) error

	// ReplaySubs re-issues every resumable subscription on a freshly
	// dialed upstream. Called by the run loop before the upstream reader
	// starts, so it never races HandleUpstreamFrame. Replay aborts on the
	// first write error and returns it; nil means every sub was re-issued.
	ReplaySubs(ctx context.Context, io sessionIO) error

	// ResumableSubs reports how many subscriptions a replay would
	// re-issue. After upstream death the engine reconnects when > 0 and
	// terminates the session otherwise.
	ResumableSubs() int
}

// sessionIO is the only engine surface adapters may touch: serialized,
// deadline-guarded writes and JSON-RPC reply helpers. Conns, locks,
// channels, and goroutines are engine-private.
type sessionIO interface {
	upstreamWrite(msg []byte) error
	clientWrite(msg []byte) error
	clientReplyResult(id json.RawMessage, result string) error
	clientReplyBool(id json.RawMessage, result bool) error
	clientReplyError(id json.RawMessage, code int, msg string) error
}

// engine owns the protocol-independent mechanics of one client WebSocket
// connection paired with one upstream connection: dialing via the
// selector, the run loop, the reader goroutines and their teardown
// contract, serialized writes with deadlines, and JSON-RPC reply helpers.
// The session lives until either side closes for good (no resumable
// subscriptions) or until ctx is cancelled.
type engine struct {
	id       string
	client   *websocket.Conn
	selector selector.Selector
	dialer   *websocket.Dialer
	adapter  protocolAdapter
	tag      string // log prefix; the kind half of SessionLabels

	upstream  atomic.Pointer[websocket.Conn]
	upBackend atomic.Value // string

	clientWriteMu   sync.Mutex
	upstreamWriteMu sync.Mutex

	closed atomic.Bool
	done   chan struct{} // closed when run exits; releases clientReader sends
}

// newEngine wires an engine to its client connection and adapter. A nil
// dialer gets a default dialer using handshakeTimeout when > 0, else 5s.
func newEngine(client *websocket.Conn, sel selector.Selector, dialer *websocket.Dialer, handshakeTimeout time.Duration, adapter protocolAdapter) *engine {
	if dialer == nil {
		if handshakeTimeout <= 0 {
			handshakeTimeout = 5 * time.Second
		}
		dialer = &websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	}
	_, tag := adapter.SessionLabels()
	return &engine{
		id:       runtime.NewRequestID(),
		client:   client,
		selector: sel,
		dialer:   dialer,
		adapter:  adapter,
		tag:      tag,
		done:     make(chan struct{}),
	}
}

// run blocks until the session terminates and returns the terminal cause.
//
// readClient is spawned as the client-reader goroutine and must pump
// frames into clientCh (terminal error into errCh) until the conn dies or
// e.done closes. Wrappers pass their own clientReader method — which
// delegates straight back to (*engine).clientReader — so goroutine stacks
// keep the historical (*Session).clientReader / (*InjSession).clientReader
// frames that the lifecycle tests probe.
func (e *engine) run(ctx context.Context, readClient func(clientCh chan<- []byte, errCh chan<- error)) error {
	proto, kind := e.adapter.SessionLabels()
	defer e.closeClient(websocket.CloseGoingAway, "session ending")
	defer metrics.SubscriptionsActive.WithLabelValues(proto, kind).Dec()
	defer close(e.done) // every return stops draining clientCh; release the reader
	metrics.SubscriptionsActive.WithLabelValues(proto, kind).Inc()

	clientCh := make(chan []byte, 32)
	clientErrCh := make(chan error, 1)
	go readClient(clientCh, clientErrCh)

	for {
		// Dial an upstream.
		if !e.dialUpstream(ctx) {
			return errors.New("no eligible upstream")
		}

		// On every (re)connect, replay resumable subscriptions before the
		// upstream reader starts consuming acks.
		//
		// TODO: treat replay write failure as upstream death (tearDownUpstream)
		// instead of waiting for the reader to notice — a poisoned write side
		// with a healthy read side leaves unreplayed subs stranded until the
		// next upstream drop.
		if err := e.adapter.ReplaySubs(ctx, e); err != nil {
			log.FromCtx(ctx).Warn(e.tag+": replay failed; continuing with partial subscriptions", "err", err.Error())
		}

		upDone := make(chan error, 1)
		go func() { upDone <- e.upstreamReader() }()

		// Forward client → upstream until either side dies.
	forward:
		for {
			select {
			case <-ctx.Done():
				e.tearDownUpstream()
				<-upDone
				return ctx.Err()
			case msg, ok := <-clientCh:
				if !ok {
					e.tearDownUpstream()
					<-upDone
					return nil
				}
				if err := e.adapter.HandleClientFrame(e, msg); err != nil {
					log.FromCtx(ctx).Debug(e.tag+": client frame route failed", "err", err.Error())
					e.tearDownUpstream()
					<-upDone
					return err
				}
			case err := <-clientErrCh:
				e.tearDownUpstream()
				<-upDone
				return err
			case upErr := <-upDone:
				resumable := e.adapter.ResumableSubs()
				log.FromCtx(ctx).Info(e.tag+": upstream gone, evaluating resume",
					"backend", e.upBackend.Load(),
					"resumable_subs", resumable,
					"err", errString(upErr),
				)
				if resumable == 0 {
					return nil
				}
				metrics.SubscriptionResumes.WithLabelValues(e.adapter.ResumeReason()).Inc()
				break forward // loop reconnects
			}
		}
	}
}

// clientReader pumps frames from the client into clientCh until close.
// Sends race e.done: once run returns nothing drains clientCh, and a conn
// close only unblocks ReadMessage — a full buffer would otherwise strand
// this goroutine on the send forever.
func (e *engine) clientReader(clientCh chan<- []byte, errCh chan<- error) {
	defer close(clientCh)
	for {
		_, msg, err := e.client.ReadMessage()
		if err != nil {
			errCh <- err // cap-1, single send ever — never blocks
			return
		}
		select {
		case clientCh <- msg:
		case <-e.done:
			return
		}
	}
}

// upstreamReader pumps frames from the current upstream into the adapter
// until the connection dies or the adapter reports an error.
func (e *engine) upstreamReader() error {
	conn := e.upstream.Load()
	if conn == nil {
		return errors.New("no upstream")
	}
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if err := e.adapter.HandleUpstreamFrame(e, msg); err != nil {
			return err
		}
	}
}

// dialUpstream walks the selector candidates for the adapter's route key
// and opens a WS to the first one that answers (5s dial timeout, 60s read
// deadline refreshed by pongs). Returns false if every candidate fails.
func (e *engine) dialUpstream(ctx context.Context) bool {
	key := e.adapter.DialRouteKey()
	candidates := e.selector.Candidates(key)
	for _, b := range candidates {
		ep := b.Endpoint(key.Protocol)
		if ep == "" {
			continue
		}
		addr := e.adapter.NormalizeEndpoint(ep)
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, _, err := e.dialer.DialContext(dialCtx, addr, nil)
		cancel()
		if err != nil {
			log.FromCtx(ctx).Warn(e.tag+": upstream dial failed", "backend", b.Name, "err", err.Error())
			continue
		}
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		})
		e.upstream.Store(conn)
		e.upBackend.Store(b.Name)
		log.FromCtx(ctx).Info(e.tag+": upstream connected", "session_id", e.id, "backend", b.Name)
		return true
	}
	return false
}

func (e *engine) tearDownUpstream() {
	if c := e.upstream.Swap(nil); c != nil {
		_ = c.Close()
	}
}

// closeClient writes a close frame and closes the underlying conn once.
func (e *engine) closeClient(code int, msg string) {
	if !e.closed.CompareAndSwap(false, true) {
		return
	}
	e.clientWriteMu.Lock()
	_ = e.client.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(code, msg),
		time.Now().Add(2*time.Second),
	)
	e.clientWriteMu.Unlock()
	_ = e.client.Close()
}

// upstreamWrite is serialized — gorilla forbids concurrent writers on a
// single connection.
func (e *engine) upstreamWrite(msg []byte) error {
	conn := e.upstream.Load()
	if conn == nil {
		return errors.New("no upstream")
	}
	e.upstreamWriteMu.Lock()
	defer e.upstreamWriteMu.Unlock()
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, msg)
}

func (e *engine) clientWrite(msg []byte) error {
	e.clientWriteMu.Lock()
	defer e.clientWriteMu.Unlock()
	if err := e.client.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return e.client.WriteMessage(websocket.TextMessage, msg)
}

func (e *engine) clientReplyResult(id json.RawMessage, result string) error {
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(id),
		"result":  result,
	})
	return e.clientWrite(out)
}

func (e *engine) clientReplyBool(id json.RawMessage, result bool) error {
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(id),
		"result":  result,
	})
	return e.clientWrite(out)
}

func (e *engine) clientReplyError(id json.RawMessage, code int, msg string) error {
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(id),
		"error": map[string]any{
			"code":    code,
			"message": msg,
		},
	})
	return e.clientWrite(out)
}

// backendName names the currently bound upstream backend.
func (e *engine) backendName() string {
	v, _ := e.upBackend.Load().(string)
	return v
}

// errString renders an error as a nil-safe log attribute (local replica of
// the helper in health/probe_eth_ws.go; not worth an import).
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
