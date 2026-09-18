// Package cmt_rpc is the CometBFT RPC listener (HTTP and WebSocket).
package cmt_rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/InjectiveLabs/stitch/internal/cache"
	"github.com/InjectiveLabs/stitch/internal/forwarder"
	"github.com/InjectiveLabs/stitch/internal/log"
	"github.com/InjectiveLabs/stitch/internal/runtime"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/server"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// Server is the CometBFT RPC listener.
type Server struct {
	addr       string
	fwd        *forwarder.HTTP
	cache      *cache.HashIndex
	respCache  *cache.ResponseCache
	head       cache.HeadProvider
	confDepth  int64
	cacheTTL   time.Duration
	srv        *http.Server
	wsSelector selector.Selector
	wsTracker  *server.ConnTracker
}

// SetHashCache attaches a shared hash→height index for memoization on
// hash-keyed methods (block_by_hash, tx, header_by_hash).
func (s *Server) SetHashCache(c *cache.HashIndex) { s.cache = c }

// SetResponseCache wires the shared response cache, head accessor, and the
// TTL applied to entries this server stores (ttl ≤ 0 stores without expiry).
func (s *Server) SetResponseCache(c *cache.ResponseCache, head cache.HeadProvider, confirmationDepth int64, ttl time.Duration) {
	s.respCache = c
	s.head = head
	s.confDepth = confirmationDepth
	s.cacheTTL = ttl
}

func New(addr string, fwd *forwarder.HTTP) *Server {
	s := &Server{addr: addr, fwd: fwd, wsTracker: server.NewConnTracker()}
	mux := http.NewServeMux()
	mux.Handle("/", s)
	mux.HandleFunc("/websocket", s.serveWebSocket)
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

func (s *Server) Shutdown(ctx context.Context) error {
	err := s.srv.Shutdown(ctx)
	if wsErr := s.wsTracker.SweepAndWait(ctx); err == nil {
		err = wsErr
	}
	return err
}

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
	// Notifications must reach the upstream without receiving another
	// caller's cached result. URI calls have no caller-selected JSON-RPC ID.
	// A URI POST body may contain form parameters, so only cache bodyless
	// URI requests whose complete parameters are available in the URL.
	cacheRequest := (!d.uri && d.version == "2.0" && cache.IsJSONRPCID(d.id)) || (d.uri && r.ContentLength == 0)
	if s.respCache != nil && cacheRequest && d.key.Cacheable && d.key.Idempotent && d.key.Class == types.ClassByHeight {
		height := d.key.HeightOrZero()
		var head int64
		if s.head != nil {
			head = s.head()
		}
		if cache.IsCacheableHeight(height, head, s.confDepth) {
			protocol := string(d.key.Protocol)
			params := []byte(d.params)
			if d.uri {
				// Encode sorts query keys while preserving repeated-value order.
				// Keep URI and JSON-RPC responses in separate namespaces.
				protocol += ":uri:" + r.Method
				params = []byte(r.URL.Query().Encode())
			}
			cacheKey := cache.BuildKey(protocol, d.key.Method, height, cache.HashParams(params))
			if hit, ok := s.respCache.Get(cacheKey); ok {
				response := hit
				valid := cmtCacheableResponse(d.key.Method, hit)
				if !d.uri && valid {
					response, valid = cache.ResponseWithID(hit, d.id)
				}
				if valid {
					w.Header().Set("content-type", "application/json")
					w.Header().Set("x-stitch-cache", "hit")
					// #nosec G705 -- Validated JSON-RPC is served as application/json.
					_, _ = w.Write(response)
					return
				}
			}
			cap := server.NewCapture(w.Header())
			cap.Header().Set("x-stitch-cache", "miss")
			dispatch(cap, r, d.key)
			cap.FlushTo(w)
			if cap.Status() >= 200 && cap.Status() < 300 && cmtCacheableResponse(d.key.Method, cap.BodyBytes()) {
				s.respCache.Set(cacheKey, cap.BodyBytes(), s.cacheTTL)
			}
			if s.cache != nil && cmtPopulatable(d.key.Method) {
				cache.PopulateFromCMTResponse(s.cache, d.key.Method, cap.BodyBytes())
			}
			return
		}
	}

	if s.cache != nil && cmtPopulatable(d.key.Method) {
		cap := server.NewCapture(w.Header())
		dispatch(cap, r, d.key)
		cap.FlushTo(w)
		cache.PopulateFromCMTResponse(s.cache, d.key.Method, cap.BodyBytes())
		return
	}
	dispatch(w, r, d.key)
}

// ABCI application errors are nested inside an otherwise successful JSON-RPC
// envelope. In particular, a missing historical version must not be cached
// after all candidate shards fail: retained state can become available later.
func cmtCacheableResponse(method string, body []byte) bool {
	if !cache.IsSuccessfulResponse(body) {
		return false
	}
	if method != "abci_query" {
		return true
	}
	var envelope struct {
		Result struct {
			Response *struct {
				Code json.RawMessage `json:"code"`
			} `json:"response"`
		} `json:"result"`
	}
	if json.Unmarshal(body, &envelope) != nil || envelope.Result.Response == nil {
		return false
	}
	code := bytes.TrimSpace(envelope.Result.Response.Code)
	if len(code) == 0 {
		return true // CometBFT may omit the zero code.
	}
	value, err := strconv.ParseUint(unquoteRaw(code), 10, 32)
	return err == nil && value == 0
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
