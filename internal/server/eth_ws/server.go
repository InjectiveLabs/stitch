// Package eth_ws is the EVM JSON-RPC WebSocket listener.
//
// As of Phase 5a, every accepted connection is delegated to a
// subscription.Session, which:
//
//   - relays non-subscribe JSON-RPC frames verbatim
//   - intercepts eth_subscribe / eth_unsubscribe to mint synthetic
//     subscription IDs, hiding upstream restarts from clients
//   - on upstream failure, re-dials a candidate, re-issues the active
//     resumable subscriptions, and dedupes events with cursor ≤ last
//     delivered (no duplicates, no client-visible reconnect)
//
// `newPendingTransactions` is mempool-local — flagged non-resumable; the
// session terminates instead of forging continuity that doesn't exist.
package eth_ws

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/runtime"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/server"
	"github.com/decentrio/stitch/internal/subscription"
	"github.com/decentrio/stitch/internal/types"
)

// Server is the EVM WebSocket listener.
type Server struct {
	addr     string
	selector selector.Selector
	upgrader websocket.Upgrader
	dialer   *websocket.Dialer
	srv      *http.Server
	tracker  *server.ConnTracker

	subOpts SubscriptionOptions
}

// SubscriptionOptions mirrors the policies.subscriptions knobs this
// listener consumes.
type SubscriptionOptions struct {
	// ReplayTimeout is the max time to wait for a dialable upstream
	// during resume before terminating the session. <= 0 means a single
	// dial pass per resume.
	ReplayTimeout time.Duration
}

func New(addr string, sel selector.Selector) *Server {
	s := &Server{
		addr:     addr,
		selector: sel,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     func(*http.Request) bool { return true }, // operators put TLS/auth in front
		},
		dialer: &websocket.Dialer{
			HandshakeTimeout: 5 * time.Second,
			ReadBufferSize:   4096,
			WriteBufferSize:  4096,
		},
		tracker: server.NewConnTracker(),
	}
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           s,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// SetSubscriptions installs the subscriptions policy. Call before Start.
func (s *Server) SetSubscriptions(o SubscriptionOptions) { s.subOpts = o }

func (s *Server) Name() string { return "eth_ws" }

func (s *Server) Start(_ context.Context) error {
	log.L().Info("eth_ws: listening", "addr", s.addr)
	if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown stops the listener, then force-closes every live WS session
// and waits — bounded by ctx — for their handlers to return. Closing the
// client conn is enough: the session's reader errors out and the run
// loop unwinds through its teardown paths, dropping the upstream dial.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.srv.Shutdown(ctx)
	if sweepErr := s.tracker.SweepAndWait(ctx); sweepErr != nil && err == nil {
		err = sweepErr
	}
	return err
}

// Handler returns the underlying http.Handler — useful for tests.
func (s *Server) Handler() http.Handler { return s.srv.Handler }

// ServeHTTP performs the WS handshake, then hands the client connection
// off to a subscription Session.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rid := runtime.NewRequestID()
	ctx := log.WithRequestID(r.Context(), rid)
	ctx = log.WithProtocol(ctx, string(types.ProtoEthWS))
	w.Header().Set("x-request-id", rid)

	// Pre-flight: refuse if no backend has eth_ws so the client gets a
	// proper HTTP error rather than a 1011 close after the upgrade.
	candidates := s.selector.Candidates(types.RouteKey{
		Protocol: types.ProtoEthWS,
		Method:   "preflight",
		Class:    types.ClassLatest,
	})
	if len(candidates) == 0 {
		http.Error(w, "no eligible backend for eth_ws", http.StatusServiceUnavailable)
		return
	}

	clientConn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.FromCtx(ctx).Warn("eth_ws: upgrade failed", "err", err.Error())
		return
	}
	if !s.tracker.Track(clientConn) {
		_ = clientConn.Close()
		return
	}
	defer s.tracker.Untrack(clientConn)

	sess := subscription.NewSession(clientConn, subscription.SessionConfig{
		Selector:      s.selector,
		Dialer:        s.dialer,
		ReplayTimeout: s.subOpts.ReplayTimeout,
	})
	if err := sess.Run(ctx); err != nil {
		log.FromCtx(ctx).Debug("eth_ws: session ended", "err", err.Error())
	}
}
