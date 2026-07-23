// Package inj_ws is the /injstream-ws JSON-RPC bridge per Injective
// query reference §5: a WebSocket endpoint serving subscribe/unsubscribe
// JSON-RPC over a wrapped ChainStream stream.
//
// What it does:
//
//   - Accepts WS connections at /injstream-ws (path is hardcoded by the
//     Injective spec).
//   - Default mode: hands the upgraded connection to a
//     subscription.InjSession which manages subscribe/unsubscribe
//     lifecycle, mints internal JSON-RPC ids, and resumes the upstream
//     subscription on backend failure while keeping client-visible ids
//     stable. Every client gets its own upstream connection.
//   - Multicast mode (policies.subscriptions.multicast, phase 5b): a
//     subscription.HubSession routes subscribe/unsubscribe through the
//     server's shared subscription.Hub, which canonicalizes filter JSON
//     and coalesces identical subscriptions onto one upstream per
//     canonical filter. Non-subscribe JSON-RPC frames are answered with
//     -32601 in this mode — there is no per-client upstream to forward
//     them to.
//
// What is intentionally absent:
//
//   - Synthetic gap envelope when the new upstream's earliest available
//     block exceeds the cursor.
package inj_ws

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/InjectiveLabs/stitch/internal/log"
	"github.com/InjectiveLabs/stitch/internal/runtime"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/server"
	"github.com/InjectiveLabs/stitch/internal/subscription"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// EndpointPath is the canonical Injective path; clients hit this URL
// regardless of where the listener is bound.
const EndpointPath = "/injstream-ws"

// Server is the /injstream-ws bridge listener.
type Server struct {
	addr     string
	selector selector.Selector
	upgrader websocket.Upgrader
	dialer   *websocket.Dialer
	srv      *http.Server
	tracker  *server.ConnTracker

	subOpts SubscriptionOptions
	hub     *subscription.Hub // non-nil iff multicast mode is on
}

// SubscriptionOptions mirrors policies.subscriptions for this listener.
type SubscriptionOptions struct {
	// Multicast coalesces clients with the same canonical filter onto one
	// shared upstream connection (subscription.Hub). Off by default:
	// every client gets its own upstream.
	Multicast bool
	// SlowConsumer is the hub fan-out policy when a client's send buffer
	// is full: drop | disconnect | backpressure. Multicast mode only.
	SlowConsumer string
	// SendBuffer is the per-subscriber send-channel capacity (multicast
	// mode only). <= 0 keeps the hub default.
	SendBuffer int
	// ReplayTimeout is the max time to wait for a dialable upstream
	// during resume before dropping the subscriber/session. <= 0 means a
	// single dial pass per resume. Applies in both modes.
	ReplayTimeout time.Duration
}

func New(addr string, sel selector.Selector) *Server {
	s := &Server{
		addr:     addr,
		selector: sel,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     func(*http.Request) bool { return true },
		},
		dialer: &websocket.Dialer{
			HandshakeTimeout: 5 * time.Second,
			ReadBufferSize:   4096,
			WriteBufferSize:  4096,
		},
		tracker: server.NewConnTracker(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc(EndpointPath, s.serveWS)
	mux.HandleFunc("/", notFound)
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// SetSubscriptions installs the subscriptions policy. Call before Start;
// not safe to call once connections are being served. Multicast mode
// constructs the server-wide hub here so every session shares it.
func (s *Server) SetSubscriptions(o SubscriptionOptions) {
	s.subOpts = o
	if !o.Multicast {
		s.hub = nil
		return
	}
	hub := subscription.NewHub(s.selector, s.dialer)
	if o.SlowConsumer != "" {
		hub.SlowConsumer = o.SlowConsumer
	}
	if o.SendBuffer > 0 {
		hub.SendBufSize = o.SendBuffer
	}
	hub.ReplayTimeout = o.ReplayTimeout
	s.hub = hub
}

func (s *Server) Name() string { return "inj_ws" }

func (s *Server) Start(_ context.Context) error {
	log.L().Info("inj_ws: listening", "addr", s.addr, "path", EndpointPath)
	if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown stops the listener, then force-closes every live WS session
// and waits — bounded by ctx — for their handlers to return. Closing the
// client conn is enough: the session's reader errors out and the run
// loop unwinds through its teardown paths, dropping the upstream dial.
// In multicast mode the shared hub is shut down after the client sweep
// (sessions detach their subscribers as they unwind; the hub teardown
// closes whatever upstreams remain and waits for their goroutines).
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.srv.Shutdown(ctx)
	if sweepErr := s.tracker.SweepAndWait(ctx); sweepErr != nil && err == nil {
		err = sweepErr
	}
	if s.hub != nil {
		if hubErr := s.hub.Shutdown(ctx); hubErr != nil && err == nil {
			err = hubErr
		}
	}
	return err
}

// Handler exposes the underlying handler — used by tests.
func (s *Server) Handler() http.Handler { return s.srv.Handler }

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	rid := runtime.NewRequestID()
	ctx := log.WithRequestID(r.Context(), rid)
	ctx = log.WithProtocol(ctx, "inj_ws")
	w.Header().Set("x-request-id", rid)

	// Pre-flight: refuse the upgrade if no chainstream backend is configured.
	candidates := s.selector.Candidates(types.RouteKey{
		Protocol: types.ProtoChainStream,
		Method:   "preflight_inj_ws",
		Class:    types.ClassLatest,
	})
	if len(candidates) == 0 {
		http.Error(w, "no eligible chainstream backend for /injstream-ws", http.StatusServiceUnavailable)
		return
	}

	clientConn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		if plainHTTPUpgradeError(err) {
			log.FromCtx(ctx).Debug("inj_ws: non-websocket request", "err", err.Error())
		} else {
			log.FromCtx(ctx).Warn("inj_ws: upgrade failed", "err", err.Error())
		}
		return
	}
	if !s.tracker.Track(clientConn) {
		_ = clientConn.Close()
		return
	}
	defer s.tracker.Untrack(clientConn)

	if s.hub != nil {
		sess := subscription.NewHubSession(clientConn, s.hub)
		if err := sess.Run(ctx); err != nil {
			log.FromCtx(ctx).Debug("inj_ws: hub session ended", "err", err.Error())
		}
		return
	}
	sess := subscription.NewInjSession(clientConn, subscription.InjSessionConfig{
		Selector:      s.selector,
		Dialer:        s.dialer,
		ReplayTimeout: s.subOpts.ReplayTimeout,
	})
	if err := sess.Run(ctx); err != nil {
		log.FromCtx(ctx).Debug("inj_ws: session ended", "err", err.Error())
	}
}

func notFound(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(`{"error":"path not found; expect ` + EndpointPath + `"}`))
}

func plainHTTPUpgradeError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "not using the websocket protocol") ||
		strings.Contains(msg, "'upgrade' token not found")
}
