// Package cosmos_rest is the Cosmos REST / gRPC-Gateway listener.
//
// Routing strategy:
//
//  1. Read x-cosmos-block-height header (canonical).
//  2. If absent, parse height from URL path (e.g. /cosmos/.../blocks/{height}).
//  3. If absent, parse from query string (?height=, ?query=tx.height=N).
//  4. Otherwise route to a "latest"-eligible backend.
package cosmos_rest

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/InjectiveLabs/stitch/internal/forwarder"
	"github.com/InjectiveLabs/stitch/internal/log"
	"github.com/InjectiveLabs/stitch/internal/runtime"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// Server is the Cosmos REST proxy.
type Server struct {
	addr string
	fwd  *forwarder.HTTP
	srv  *http.Server
}

func New(addr string, fwd *forwarder.HTTP) *Server {
	s := &Server{addr: addr, fwd: fwd}
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           s,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

func (s *Server) Name() string { return "cosmos_rest" }

func (s *Server) Start(_ context.Context) error {
	log.L().Info("cosmos_rest: listening", "addr", s.addr)
	if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

// Handler exposes the underlying handler for tests.
func (s *Server) Handler() http.Handler { return s.srv.Handler }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rid := runtime.NewRequestID()
	ctx := log.WithRequestID(r.Context(), rid)
	ctx = log.WithProtocol(ctx, string(types.ProtoAPI))
	ctx = log.WithMethod(ctx, r.URL.Path)
	w.Header().Set("x-request-id", rid)

	key := types.RouteKey{
		Protocol:   types.ProtoAPI,
		Method:     r.URL.Path,
		Class:      types.ClassLatest,
		Idempotent: r.Method == http.MethodGet,
	}
	if h, ok := extractHeight(r); ok {
		key.Height = &h
		key.Class = types.ClassByHeight
		key.Cacheable = true
	}

	r = r.WithContext(ctx)
	s.fwd.Forward(w, r, key)
}

var heightInPath = regexp.MustCompile(`/blocks/(\d+)(?:/|$)`)
var queryTxHeight = regexp.MustCompile(`tx\.height\s*=\s*(\d+)`)

// extractHeight pulls a block height out of, in order: the canonical
// x-cosmos-block-height header, common URL path segments, and the
// `?query=tx.height=N` shape used by /cosmos/tx/v1beta1/txs.
func extractHeight(r *http.Request) (int64, bool) {
	if v := r.Header.Get("x-cosmos-block-height"); v != "" {
		if h, err := strconv.ParseInt(v, 10, 64); err == nil && h > 0 {
			return h, true
		}
	}
	if m := heightInPath.FindStringSubmatch(r.URL.Path); len(m) == 2 {
		if h, err := strconv.ParseInt(m[1], 10, 64); err == nil && h > 0 {
			return h, true
		}
	}
	q := r.URL.Query()
	if v := q.Get("height"); v != "" {
		if h, err := strconv.ParseInt(v, 10, 64); err == nil && h > 0 {
			return h, true
		}
	}
	if v := q.Get("query"); v != "" {
		if m := queryTxHeight.FindStringSubmatch(v); len(m) == 2 {
			if h, err := strconv.ParseInt(m[1], 10, 64); err == nil && h > 0 {
				return h, true
			}
		}
	}
	if strings.Contains(r.URL.RawQuery, "tx.height") {
		if m := queryTxHeight.FindStringSubmatch(r.URL.RawQuery); len(m) == 2 {
			if h, err := strconv.ParseInt(m[1], 10, 64); err == nil && h > 0 {
				return h, true
			}
		}
	}
	return 0, false
}
