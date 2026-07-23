// Package admin serves /healthz, /readyz, /metrics, and /admin/* on a single
// HTTP listener. The /admin/* endpoints are stubs in phase 0 and return 501.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/InjectiveLabs/stitch/internal/log"
	"github.com/InjectiveLabs/stitch/internal/metrics"
)

// Server hosts the admin/observability endpoints.
type Server struct {
	addr  string
	srv   *http.Server
	ready atomic.Bool
	deps  Deps
}

func New(addr string) *Server {
	s := &Server{addr: addr}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/admin/", notImplemented)
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Handler exposes the underlying handler — used by tests so callers can
// stand up an httptest.Server around the real mux.
func (s *Server) Handler() http.Handler { return s.srv.Handler }

func (s *Server) Name() string { return "admin" }

func (s *Server) Start(_ context.Context) error {
	log.L().Info("admin: listening", "addr", s.addr)
	if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// MarkReady flips /readyz from 503 → 200. Call once cross-cutting init has
// completed.
func (s *Server) MarkReady() { s.ready.Store(true) }

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not_ready"}`))
		return
	}
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

func notImplemented(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_, _ = w.Write([]byte(`{"error":"admin endpoint not yet implemented (phase 8)"}`))
}
