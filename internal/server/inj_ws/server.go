// Package inj_ws is the /injstream-ws JSON-RPC bridge per Injective
// query reference §5: a WebSocket endpoint serving subscribe/unsubscribe
// JSON-RPC over a wrapped ChainStream stream.
//
// What this phase does (5c):
//
//   - Accepts WS connections at /injstream-ws (path is hardcoded by the
//     Injective spec).
//   - Hands the upgraded connection to a subscription.InjSession which
//     manages subscribe/unsubscribe lifecycle, mints internal JSON-RPC
//     ids, and resumes the upstream subscription on backend failure
//     while keeping client-visible ids stable.
//
// What is intentionally absent:
//
//   - Multicast: every client gets its own upstream connection. Phase
//     5d will canonicalize filter JSON (sort repeated string slots) and
//     coalesce identical subscriptions onto a shared upstream.
//   - Synthetic gap envelope when the new upstream's earliest available
//     block exceeds the cursor.
package inj_ws

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/runtime"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/subscription"
	"github.com/decentrio/stitch/internal/types"
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

	// Live-session accounting. http.Server.Shutdown neither waits for
	// nor closes hijacked conns, so Shutdown sweeps these itself.
	mu       sync.Mutex
	draining bool
	sessions map[*websocket.Conn]struct{}
	handlers sync.WaitGroup
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
		sessions: make(map[*websocket.Conn]struct{}),
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
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.srv.Shutdown(ctx)

	s.mu.Lock()
	s.draining = true
	for c := range s.sessions {
		_ = c.Close()
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() { s.handlers.Wait(); close(done) }()
	select {
	case <-done:
		return err
	case <-ctx.Done():
		if err != nil {
			return err
		}
		return ctx.Err()
	}
}

// track registers a live session conn. Returns false once Shutdown has
// begun — the caller must close the conn instead of serving it, because
// the sweep may already have run.
func (s *Server) track(c *websocket.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return false
	}
	s.sessions[c] = struct{}{}
	s.handlers.Add(1)
	return true
}

func (s *Server) untrack(c *websocket.Conn) {
	s.mu.Lock()
	delete(s.sessions, c)
	s.mu.Unlock()
	s.handlers.Done()
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
		log.FromCtx(ctx).Warn("inj_ws: upgrade failed", "err", err.Error())
		return
	}
	if !s.track(clientConn) {
		_ = clientConn.Close()
		return
	}
	defer s.untrack(clientConn)

	sess := subscription.NewInjSession(clientConn, subscription.InjSessionConfig{
		Selector: s.selector,
		Dialer:   s.dialer,
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
