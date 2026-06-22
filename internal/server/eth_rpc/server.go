package eth_rpc

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
	"github.com/decentrio/stitch/internal/server"
	"github.com/decentrio/stitch/internal/types"
)

// Server is the EVM JSON-RPC HTTP listener.
type Server struct {
	addr      string
	fwd       *forwarder.HTTP
	manifest  *Manifest
	filters   *FilterStore
	cache     *cache.HashIndex
	respCache *cache.ResponseCache
	head      cache.HeadProvider
	confDepth int64
	cacheTTL  time.Duration
	dangerous *DangerousAllowlist
	// hedgeEnabled/hedgeMethods gate hedged dispatch on top of the
	// manifest's per-method hedge flag; see SetHedging.
	hedgeEnabled bool
	hedgeMethods map[string]struct{}
	srv          *http.Server
}

func New(addr string, fwd *forwarder.HTTP) *Server {
	s := &Server{
		addr:     addr,
		fwd:      fwd,
		manifest: DefaultManifest,
		filters:  NewFilterStore(5 * time.Minute),
	}
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           s,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// SetHashCache attaches a shared hash→height index. When set, the handler
// (a) consults it before forwarding to convert hash-keyed routes into
// height-keyed routes, and (b) populates it from successful responses.
func (s *Server) SetHashCache(c *cache.HashIndex) { s.cache = c }

// SetResponseCache wires a shared response-body cache, the chain-head
// accessor used to gate cacheability by confirmation depth, and the TTL
// applied to entries this server stores (ttl ≤ 0 stores without expiry).
func (s *Server) SetResponseCache(c *cache.ResponseCache, head cache.HeadProvider, confirmationDepth int64, ttl time.Duration) {
	s.respCache = c
	s.head = head
	s.confDepth = confirmationDepth
	s.cacheTTL = ttl
}

// SetHedging gates hedged dispatch. Hedging stays off (the default) until
// enabled here; an empty methods list lets every manifest-flagged method
// hedge, a non-empty list restricts hedging to methods present in BOTH the
// manifest flag and the list. Call before the listener starts serving.
func (s *Server) SetHedging(enabled bool, methods []string) {
	s.hedgeEnabled = enabled
	s.hedgeMethods = nil
	if len(methods) > 0 {
		s.hedgeMethods = make(map[string]struct{}, len(methods))
		for _, m := range methods {
			s.hedgeMethods[m] = struct{}{}
		}
	}
}

// applyHedgePolicy clears the decoded hedge flag unless operator config
// allows this method to hedge.
func (s *Server) applyHedgePolicy(d *decoded) {
	if !d.key.Hedge {
		return
	}
	if !s.hedgeEnabled {
		d.key.Hedge = false
		return
	}
	if s.hedgeMethods != nil {
		if _, ok := s.hedgeMethods[d.method]; !ok {
			d.key.Hedge = false
		}
	}
}

// SetDangerousAllowlist installs the operator-provided opt-in list of
// dangerous methods (debug_*, personal_*, miner_*). Without one, every
// dangerous method returns -32601.
func (s *Server) SetDangerousAllowlist(d *DangerousAllowlist) { s.dangerous = d }

func (s *Server) Name() string { return "eth_rpc" }

func (s *Server) Start(_ context.Context) error {
	log.L().Info("eth_rpc: listening", "addr", s.addr)
	if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

// Handler exposes the underlying handler for tests.
func (s *Server) Handler() http.Handler { return s.srv.Handler }

// FilterStore exposes the store for diagnostics and tests.
func (s *Server) FilterStore() *FilterStore { return s.filters }

// ServeHTTP routes one JSON-RPC request (single or batched).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rid := runtime.NewRequestID()
	ctx := log.WithRequestID(r.Context(), rid)
	ctx = log.WithProtocol(ctx, string(types.ProtoEthRPC))
	w.Header().Set("x-request-id", rid)

	if r.Method != http.MethodPost {
		writeRawError(w, http.StatusMethodNotAllowed, -32600, "method not allowed (use POST)")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8*1024*1024))
	_ = r.Body.Close()
	if err != nil {
		writeRawError(w, http.StatusBadRequest, -32700, "read body: "+err.Error())
		return
	}

	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		writeRawError(w, http.StatusBadRequest, -32700, "empty body")
		return
	}

	if trimmed[0] == '[' {
		s.handleBatch(w, r.WithContext(ctx), trimmed)
		return
	}
	s.handleSingle(w, r.WithContext(ctx), trimmed)
}

// handleSingle dispatches one JSON-RPC envelope. Two outcomes:
//
//   - decoded.fatal != nil: write a JSON-RPC error response (e.g. for
//     unsupported subscribe over HTTP).
//   - otherwise: forward via the HTTP forwarder, replaying body if needed.
func (s *Server) handleSingle(w http.ResponseWriter, r *http.Request, body []byte) {
	d, _, err := decodeOne(s.manifest, body)
	if err != nil {
		writeRawError(w, http.StatusBadRequest, -32700, "parse: "+err.Error())
		return
	}
	if d.spec.Hidden && !s.dangerous.Allowed(d.method) {
		writeJSONRPCError(w, d.id, -32601, "method not found")
		return
	}
	if d.fatal != nil {
		writeJSONRPCError(w, d.id, d.fatal.code, d.fatal.message)
		return
	}
	ctx := log.WithMethod(r.Context(), d.method)

	// Operator config gates hedging on top of the manifest flag.
	s.applyHedgePolicy(&d)

	// Hash-memo fast path: if the cache knows the height for this hash,
	// rewrite to a height-keyed route. The selector picks the cheapest
	// backend whose coverage includes the height, instead of having to
	// pick one that serves "latest" and hope it has the historical data.
	s.applyHashCache(&d)

	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))

	if d.expectFilterMint || d.followFilterID != "" {
		s.handleFilterCall(w, r.WithContext(ctx), d, body)
		return
	}

	dispatch := s.fwd.Forward
	switch {
	case d.key.Class == types.ClassBroadcast:
		dispatch = s.fwd.Broadcast
	case d.key.Hedge && d.key.Idempotent:
		dispatch = s.fwd.Hedge
	}

	// Response-cache fast path: if this is a cacheable, height-keyed,
	// idempotent read at a finalized height, try the cache first.
	if s.respCache != nil && d.key.Cacheable && d.key.Idempotent && d.key.Class == types.ClassByHeight {
		height := d.key.HeightOrZero()
		var head int64
		if s.head != nil {
			head = s.head()
		}
		if cache.IsCacheableHeight(height, head, s.confDepth) {
			cacheKey := cache.BuildKey(string(d.key.Protocol), d.method, height, cache.HashParams(body))
			if hit, ok := s.respCache.Get(cacheKey); ok {
				w.Header().Set("content-type", "application/json")
				w.Header().Set("x-stitch-cache", "hit")
				_, _ = w.Write(hit)
				return
			}
			// Miss — capture, forward, populate.
			cap := server.NewCapture(w.Header())
			cap.Header().Set("x-stitch-cache", "miss")
			dispatch(cap, r.WithContext(ctx), d.key)
			cap.FlushTo(w)
			if cap.Status() >= 200 && cap.Status() < 300 {
				s.respCache.Set(cacheKey, cap.BodyBytes(), s.cacheTTL)
			}
			if s.cache != nil && shouldPopulateCache(d.method) {
				cache.PopulateFromEthResponse(s.cache, d.method, cap.BodyBytes())
			}
			return
		}
	}

	// If the method's response shape is one we know how to harvest for
	// the hash index, capture the response so we can populate after
	// forwarding it through to the client.
	if s.cache != nil && shouldPopulateCache(d.method) {
		cap := server.NewCapture(w.Header())
		dispatch(cap, r.WithContext(ctx), d.key)
		cap.FlushTo(w)
		cache.PopulateFromEthResponse(s.cache, d.method, cap.BodyBytes())
		return
	}

	dispatch(w, r.WithContext(ctx), d.key)
}

// applyHashCache rewrites a ClassByHash route to ClassByHeight if we
// know the height for this hash.
func (s *Server) applyHashCache(d *decoded) {
	if s.cache == nil || d.key.Class != types.ClassByHash || len(d.key.Hash) == 0 {
		return
	}
	key := ethHashCacheKey(d.method, string(d.key.Hash))
	if key == "" {
		return
	}
	if h, ok := s.cache.Get(key); ok {
		d.key.Height = &h
		d.key.Class = types.ClassByHeight
	}
}

// ethHashCacheKey returns the cache namespace + hash for a method whose
// hash_param is set in the manifest. Empty string means "don't cache for
// this method".
func ethHashCacheKey(method, hash string) string {
	switch method {
	case "eth_getBlockByHash",
		"eth_getBlockTransactionCountByHash",
		"eth_getTransactionByBlockHashAndIndex",
		"eth_getLogs":
		return cache.EthBlockKey(hash)
	case "eth_getTransactionByHash",
		"eth_getTransactionReceipt",
		"eth_getTransactionLogs",
		"inj_getTxHashByEthHash":
		return cache.EthTxKey(hash)
	}
	return ""
}

// shouldPopulateCache reports whether method's response is worth parsing
// for hash↔height bindings.
func shouldPopulateCache(method string) bool {
	switch method {
	case "eth_getBlockByNumber",
		"eth_getBlockByHash",
		"eth_getTransactionByHash",
		"eth_getTransactionReceipt",
		"eth_getBlockReceipts":
		return true
	}
	return false
}

// handleBatch dispatches each batched request sequentially, preserving
// JSON-RPC v2's required result-order. Phase 6 may parallelize; sequential
// is correct and avoids racing on the captured header map.
func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request, body []byte) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		writeRawError(w, http.StatusBadRequest, -32700, "batch parse: "+err.Error())
		return
	}
	if len(raw) == 0 {
		writeRawError(w, http.StatusBadRequest, -32600, "empty batch")
		return
	}

	parentHeader := w.Header().Clone()
	w.Header().Set("content-type", "application/json")
	w.Header().Set("cache-control", "no-store")
	results := make([]json.RawMessage, len(raw))

	for i := range raw {
		rec := server.NewCapture(parentHeader)
		s.handleSingle(rec, r, raw[i])
		results[i] = rec.BodyBytes()
	}

	w.WriteHeader(http.StatusOK)
	out := bytes.Buffer{}
	out.WriteByte('[')
	for i, r := range results {
		if i > 0 {
			out.WriteByte(',')
		}
		// Guard against a captured response that wasn't valid JSON: wrap.
		if !json.Valid(r) {
			out.Write([]byte(`{"jsonrpc":"2.0","error":{"code":-32603,"message":"empty response"},"id":null}`))
			continue
		}
		out.Write(r)
	}
	out.WriteByte(']')
	_, _ = w.Write(out.Bytes())
}

// handleFilterCall wraps the upstream call to bind/lookup filter IDs in
// the FilterStore. The current implementation:
//
//   - For follow calls: routes via the standard forwarder (no per-backend
//     pinning yet). Sufficient as long as filters are short-lived; phase 6
//     adds proper pinning via a "preferred-backend" hint to the selector.
//   - For mint calls: parses the response, captures the resulting id, and
//     binds it. The body is buffered so we can both write to the client
//     and parse for our own use.
func (s *Server) handleFilterCall(w http.ResponseWriter, r *http.Request, d decoded, body []byte) {
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	cap := server.NewCapture(w.Header())
	s.fwd.Forward(cap, r, d.key)
	cap.FlushTo(w)

	if d.expectFilterMint {
		// Parse the captured response; if it's a JSON-RPC ok with a string
		// result, bind that id to whatever backend the forwarder chose.
		var resp struct {
			Result string `json:"result"`
		}
		if err := json.Unmarshal(cap.BodyBytes(), &resp); err == nil && resp.Result != "" {
			// We don't currently know which backend served the mint; the
			// FilterStore binds to "_unknown_" so phase 3 still tracks the
			// id but routes via the standard selector for now. Phase 6 will
			// thread the chosen backend back through the forwarder.
			s.filters.Bind(resp.Result, "_unknown_")
		}
	}
	if d.followFilterID != "" && d.method == "eth_uninstallFilter" {
		s.filters.Forget(d.followFilterID)
	}
}

// JSON-RPC framing helpers ---------------------------------------------

func writeRawError(w http.ResponseWriter, status, code int, msg string) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(jsonRPCErrorBytes(nil, code, msg))
}

func writeJSONRPCError(w http.ResponseWriter, id json.RawMessage, code int, msg string) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(jsonRPCErrorBytes(id, code, msg))
}

func jsonRPCErrorBytes(id json.RawMessage, code int, msg string) []byte {
	type rpcErr struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	type rpcResp struct {
		JSONRPC string          `json:"jsonrpc"`
		Error   rpcErr          `json:"error"`
		ID      json.RawMessage `json:"id"`
	}
	if id == nil {
		id = json.RawMessage("null")
	}
	b, _ := json.Marshal(rpcResp{JSONRPC: "2.0", Error: rpcErr{Code: code, Message: msg}, ID: id})
	return b
}
