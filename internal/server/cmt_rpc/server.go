// Package cmt_rpc is the CometBFT RPC listener (URI + JSON-RPC over HTTP).
// WebSocket support arrives in phase 5 with the subscription hub.
package cmt_rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/decentrio/stitch/internal/cache"
	"github.com/decentrio/stitch/internal/forwarder"
	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/runtime"
	"github.com/decentrio/stitch/internal/types"
)

// Server is the CometBFT RPC listener.
type Server struct {
	addr      string
	fwd       *forwarder.HTTP
	cache     *cache.HashIndex
	respCache *cache.ResponseCache
	head      cache.HeadProvider
	confDepth int64
	srv       *http.Server
}

// SetHashCache attaches a shared hash→height index for memoization on
// hash-keyed methods (block_by_hash, tx, header_by_hash).
func (s *Server) SetHashCache(c *cache.HashIndex) { s.cache = c }

// SetResponseCache wires the shared response cache and head accessor.
func (s *Server) SetResponseCache(c *cache.ResponseCache, head cache.HeadProvider, confirmationDepth int64) {
	s.respCache = c
	s.head = head
	s.confDepth = confirmationDepth
}

func New(addr string, fwd *forwarder.HTTP) *Server {
	s := &Server{addr: addr, fwd: fwd}
	mux := http.NewServeMux()
	mux.Handle("/", s)
	mux.HandleFunc("/websocket", websocketStub)
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

func (s *Server) Name() string { return "cmt_rpc" }

func (s *Server) Start(_ context.Context) error {
	log.L().Info("cmt_rpc: listening", "addr", s.addr)
	if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

// Handler returns the http.Handler the listener uses. Exported for tests
// (httptest.NewServer needs a handler, not a *http.Server).
func (s *Server) Handler() http.Handler { return s.srv.Handler }

// ServeHTTP routes a single CometBFT RPC request (URI or JSON-RPC).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rid := runtime.NewRequestID()
	ctx := log.WithRequestID(r.Context(), rid)
	ctx = log.WithProtocol(ctx, string(types.ProtoRPC))
	w.Header().Set("x-request-id", rid)

	d, err := decode(r)
	if err != nil {
		writeJSONRPCError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx = log.WithMethod(ctx, d.key.Method)
	r = r.WithContext(ctx)

	if len(d.body) > 0 {
		r.Body = io.NopCloser(bytes.NewReader(d.body))
		r.ContentLength = int64(len(d.body))
	}

	// Hash-memo fast path: convert ClassByHash → ClassByHeight when known.
	s.applyHashCache(&d)

	log.FromCtx(ctx).Debug("cmt_rpc request",
		"class", d.key.Class.String(),
		"height", d.key.HeightOrZero(),
		"idempotent", d.key.Idempotent,
	)

	dispatch := s.fwd.Forward
	if d.key.Class == types.ClassBroadcast {
		dispatch = s.fwd.Broadcast
	}

	// Response-cache fast path on cacheable + height-keyed reads.
	if s.respCache != nil && d.key.Cacheable && d.key.Idempotent && d.key.Class == types.ClassByHeight {
		height := d.key.HeightOrZero()
		var head int64
		if s.head != nil {
			head = s.head()
		}
		if cache.IsCacheableHeight(height, head, s.confDepth) {
			cacheKey := cache.BuildKey(string(d.key.Protocol), d.key.Method, height, cache.HashParams(d.body))
			if hit, ok := s.respCache.Get(cacheKey); ok {
				w.Header().Set("content-type", "application/json")
				w.Header().Set("x-stitch-cache", "hit")
				_, _ = w.Write(hit)
				return
			}
			cap := newCMTCapture(w.Header())
			cap.Header().Set("x-stitch-cache", "miss")
			dispatch(cap, r, d.key)
			cap.flushTo(w)
			if cap.status >= 200 && cap.status < 300 {
				s.respCache.Set(cacheKey, cap.body.Bytes(), 5*time.Minute)
			}
			if s.cache != nil && cmtPopulatable(d.key.Method) {
				cache.PopulateFromCMTResponse(s.cache, d.key.Method, cap.body.Bytes())
			}
			return
		}
	}

	if s.cache != nil && cmtPopulatable(d.key.Method) {
		cap := newCMTCapture(w.Header())
		dispatch(cap, r, d.key)
		cap.flushTo(w)
		cache.PopulateFromCMTResponse(s.cache, d.key.Method, cap.body.Bytes())
		return
	}
	dispatch(w, r, d.key)
}

func (s *Server) applyHashCache(d *decoded) {
	if s.cache == nil || d.key.Class != types.ClassByHash || len(d.key.Hash) == 0 {
		return
	}
	key := cmtHashCacheKey(d.key.Method, string(d.key.Hash))
	if key == "" {
		return
	}
	if h, ok := s.cache.Get(key); ok {
		d.key.Height = &h
		d.key.Class = types.ClassByHeight
	}
}

func cmtHashCacheKey(method, hash string) string {
	switch method {
	case "block_by_hash", "header_by_hash":
		return cache.CMTBlockKey(hash)
	case "tx":
		return cache.CMTTxKey(hash)
	}
	return ""
}

func cmtPopulatable(method string) bool {
	switch method {
	case "block", "block_by_hash", "tx":
		return true
	}
	return false
}

// cmtCapture buffers the upstream response so we can both write it
// through to the client and parse it for hash↔height bindings. Mirrors
// eth_rpc's capture; kept separate to avoid an import cycle.
type cmtCapture struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newCMTCapture(parent http.Header) *cmtCapture {
	return &cmtCapture{header: parent.Clone(), status: 200}
}

func (c *cmtCapture) Header() http.Header         { return c.header }
func (c *cmtCapture) WriteHeader(code int)        { c.status = code }
func (c *cmtCapture) Write(b []byte) (int, error) { return c.body.Write(b) }

func (c *cmtCapture) flushTo(w http.ResponseWriter) {
	for k, vs := range c.header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(c.status)
	_, _ = w.Write(c.body.Bytes())
}

func websocketStub(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_, _ = w.Write([]byte(`{"error":"/websocket arrives in phase 5 (subscription hub)"}`))
}

func writeJSONRPCError(w http.ResponseWriter, status int, msg string) {
	type rpcErr struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	type rpcResp struct {
		JSONRPC string  `json:"jsonrpc"`
		Error   rpcErr  `json:"error"`
		ID      *string `json:"id"`
	}
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(rpcResp{
		JSONRPC: "2.0",
		Error:   rpcErr{Code: -32600, Message: msg},
	})
}
