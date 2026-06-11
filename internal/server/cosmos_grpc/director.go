package cosmos_grpc

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/pool"
	"github.com/decentrio/stitch/internal/runtime"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/types"
)

// HeightHeader is the canonical Cosmos metadata key for height routing.
const HeightHeader = "x-cosmos-block-height"

// chosenBackendKey is the context key the director writes the chosen
// backend name into so the post-call wrapper can credit the breaker.
type ctxKey int

const chosenBackendKey ctxKey = iota

// chosenSlot returns the per-RPC choice slot the wrapper installs, or nil
// if no slot is present (e.g. unit tests calling the director directly).
func chosenSlot(ctx context.Context) *atomicString {
	v, _ := ctx.Value(chosenBackendKey).(*atomicString)
	return v
}

// atomicString is a tiny mutable string slot — cheaper than sync.Map for
// per-RPC state.
type atomicString struct {
	mu  sync.Mutex
	val string
}

func (a *atomicString) Set(s string) {
	a.mu.Lock()
	a.val = s
	a.mu.Unlock()
}

func (a *atomicString) Get() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.val
}

// Director is the mwitkow/grpc-proxy director. Per RPC, it:
//
//  1. Stamps a request id and pushes log scope.
//  2. Reads x-cosmos-block-height from incoming metadata.
//  3. Builds a RouteKey based on the method name (broadcast methods are
//     non-idempotent; everything else is idempotent and may be retried).
//  4. Asks the selector for ordered candidates.
//  5. Tries each — circuit-admitted (Acquire) and dial-able — until one
//     succeeds.
//
// mwitkow's TransparentHandler will replay the bidirectional stream onto
// the chosen connection. Note: streaming RPCs cannot be transparently
// retried after partial response delivery; failover only applies before
// the first message lands. Phase 5's subscription hub adds proper resume.
type Director struct {
	selector selector.Selector
	circuit  *circuit.Manager
	pool     *pool.GRPCPool
}

func NewDirector(s selector.Selector, c *circuit.Manager, p *pool.GRPCPool) *Director {
	return &Director{selector: s, circuit: c, pool: p}
}

// Direct chooses an upstream for fullMethodName. Returns the modified
// outgoing context, the gRPC conn to dial, or a status error.
//
// Committing to a backend claims a circuit admission (Acquire); every
// admission is resolved by exactly one Record or Release — dial failures
// here, RPC outcomes via the post-call wrapper (streamHandler →
// RecordOutcome / ReleaseOutcome). Direct must therefore only run under
// streamHandler, which installs the chosen-backend slot it reports
// through.
func (d *Director) Direct(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
	rid := runtime.NewRequestID()
	ctx = log.WithRequestID(ctx, rid)
	ctx = log.WithProtocol(ctx, string(types.ProtoGRPC))
	ctx = log.WithMethod(ctx, fullMethodName)

	md, _ := metadata.FromIncomingContext(ctx)
	key := buildRouteKey(fullMethodName, md)

	candidates := d.selector.Candidates(key)
	if len(candidates) == 0 {
		log.FromCtx(ctx).Warn("grpc: no eligible candidates", "method", fullMethodName)
		metrics.RequestsTotal.WithLabelValues(string(types.ProtoGRPC), key.Class.String(), "-", "no_candidates").Inc()
		return ctx, nil, status.Error(codes.Unavailable, "no eligible backend")
	}

	for _, b := range candidates {
		ep := b.Endpoint(types.ProtoGRPC)
		if ep == "" {
			continue
		}
		// Re-check the breaker at commit time: it may have tripped between
		// selection and now. Acquire also claims the single half-open
		// canary slot; a skipped candidate claims nothing.
		if !d.circuit.Acquire(b.Name, types.ProtoGRPC) {
			continue
		}
		conn, err := d.pool.Conn(ctx, b.Name, pool.CleanAddr(ep))
		if err != nil {
			d.circuit.Record(b.Name, types.ProtoGRPC, false)
			log.FromCtx(ctx).Warn("grpc: dial failed", "backend", b.Name, "err", err.Error())
			metrics.FailoverAttempts.WithLabelValues(b.Name, "next", "dial").Inc()
			continue
		}
		ctx = log.WithBackend(ctx, b.Name)
		if slot := chosenSlot(ctx); slot != nil {
			slot.Set(b.Name)
		}
		// Forward incoming metadata to upstream verbatim.
		out := metadata.NewOutgoingContext(ctx, md.Copy())
		// Stamp a tracing-style request id so upstream logs correlate.
		out = metadata.AppendToOutgoingContext(out, "x-stitch-request-id", rid)
		metrics.RequestsTotal.WithLabelValues(string(types.ProtoGRPC), key.Class.String(), b.Name, "directed").Inc()
		return out, conn, nil
	}

	log.FromCtx(ctx).Error("grpc: all candidates exhausted", "method", fullMethodName)
	metrics.RequestsTotal.WithLabelValues(string(types.ProtoGRPC), key.Class.String(), "-", "all_failed").Inc()
	return ctx, nil, status.Error(codes.Unavailable, "all candidates failed")
}

// RecordOutcome lets the gRPC server tell the director whether the actual
// RPC succeeded; the director feeds the circuit breaker, resolving the
// admission claimed in Direct. Called from the post-call wrapper.
func (d *Director) RecordOutcome(backend string, success bool) {
	if backend == "" {
		return
	}
	d.circuit.Record(backend, types.ProtoGRPC, success)
}

// ReleaseOutcome abandons the admission claimed in Direct without
// recording a sample — for RPCs whose outcome says nothing about the
// backend (the client vanished mid-stream). Frees a claimed half-open
// canary slot; mirrors the forwarder's Release convention.
func (d *Director) ReleaseOutcome(backend string) {
	if backend == "" {
		return
	}
	d.circuit.Release(backend, types.ProtoGRPC)
}

// buildRouteKey reads metadata + method name and produces a routing
// decision. Broadcast detection is by method-name suffix.
func buildRouteKey(fullMethod string, md metadata.MD) types.RouteKey {
	key := types.RouteKey{
		Protocol:   types.ProtoGRPC,
		Method:     fullMethod,
		Class:      types.ClassLatest,
		Idempotent: true,
	}
	// Broadcast / write detection by service+method:
	//   /cosmos.tx.v1beta1.Service/BroadcastTx is the canonical write.
	if isBroadcast(fullMethod) {
		key.Class = types.ClassBroadcast
		key.Idempotent = false
	}
	if vs := md.Get(HeightHeader); len(vs) > 0 {
		if h, err := strconv.ParseInt(vs[0], 10, 64); err == nil && h > 0 {
			key.Height = &h
			key.Class = types.ClassByHeight
		}
	}
	return key
}

func isBroadcast(method string) bool {
	// Strip leading slash and look for known broadcast methods. Phase 6 adds
	// proper fan-out; until then, broadcast routes to one healthy backend
	// without retry.
	m := strings.TrimPrefix(method, "/")
	switch m {
	case "cosmos.tx.v1beta1.Service/BroadcastTx",
		"injective.exchange.v1beta1.Msg/BroadcastTx",
		"injective.exchange.v2.Msg/BroadcastTx":
		return true
	}
	return strings.HasSuffix(m, "/BroadcastTx")
}
