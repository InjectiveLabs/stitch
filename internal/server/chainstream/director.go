package chainstream

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/pool"
	"github.com/decentrio/stitch/internal/runtime"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/subscription"
	"github.com/decentrio/stitch/internal/types"
)

// Director runs one ChainStream call: receive the StreamRequest, then
// loop dial-and-relay across candidates — with jittered exponential
// backoff between attempts — deduping by cursor when the upstream
// changes.
type Director struct {
	selector selector.Selector
	circuit  *circuit.Manager
	pool     *pool.GRPCPool

	// Resume-backoff knobs — overridable in tests to tighten the loop
	// (same pattern as health.EthWSProber's). Set in NewDirector.
	baseBackoff time.Duration
	maxBackoff  time.Duration

	rand *rand.Rand
	mu   sync.Mutex // guards rand (rand.Rand is not goroutine-safe)
}

func NewDirector(s selector.Selector, c *circuit.Manager, p *pool.GRPCPool) *Director {
	return &Director{
		selector:    s,
		circuit:     c,
		pool:        p,
		baseBackoff: 250 * time.Millisecond,
		maxBackoff:  5 * time.Second,
		rand:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Handler returns a grpc.StreamHandler suitable for
// grpc.UnknownServiceHandler. One handler invocation = one client stream.
func (d *Director) Handler() grpc.StreamHandler {
	return func(_ any, ss grpc.ServerStream) error {
		return d.handle(ss)
	}
}

// handle implements the resume loop. Returns nil on clean termination,
// or a status error on terminal failure.
func (d *Director) handle(ss grpc.ServerStream) error {
	method, ok := grpc.MethodFromServerStream(ss)
	if !ok {
		return status.Error(codes.Internal, "missing method")
	}
	if !isChainStreamMethod(method) {
		// Not a stream method — refuse rather than guess.
		return status.Errorf(codes.Unimplemented, "unsupported method %q on chainstream listener", method)
	}

	rid := runtime.NewRequestID()
	ctx := log.WithRequestID(ss.Context(), rid)
	ctx = log.WithProtocol(ctx, string(types.ProtoChainStream))
	ctx = log.WithMethod(ctx, method)

	// Phase 1: receive the (single) StreamRequest from the client.
	var req RawMessage
	if err := ss.RecvMsg(&req); err != nil {
		return err // client closed / context cancelled
	}

	log.FromCtx(ctx).Info("chainstream: client opened",
		"method", method,
		"request_bytes", len(req.Bytes),
	)

	var cursor uint64

	// maxAttempts bounds the dial-and-relay resume loop. With the jittered
	// exponential backoff between attempts (baseBackoff doubling to the
	// maxBackoff cap), the 8 attempts span roughly 30s worst case at the
	// default 250ms→5s knobs (~18s of nominal sleep, plus jitter and
	// per-attempt dial/stream time) instead of hammering a flapping
	// upstream 8 times instantly.
	const maxAttempts = 8
	attemptsLeft := maxAttempts
	var lastErr error
	var lastFailed string     // backend whose stream failed last; deprioritized on the next pick
	delay := time.Duration(0) // first attempt: immediate

	for attemptsLeft > 0 {
		attemptsLeft--

		if delay > 0 {
			select {
			case <-ctx.Done():
				// Client went away while we waited to re-dial: nothing
				// left to relay to (same outcome as errClientGone).
				return nil
			case <-time.After(delay):
			}
		}

		backend, conn := d.pickConn(ctx, lastFailed)
		if conn == nil {
			lastErr = errors.New("no eligible backend")
			break
		}

		// pickConn left an admission claimed for backend; resolve it with
		// exactly one Record or Release per outcome below.
		err := d.runOnce(ctx, ss, conn, method, req, &cursor, backend)
		switch {
		case err == nil:
			// Upstream finished cleanly (server-side EOF): credit the
			// admission and pass the EOF through.
			d.circuit.Record(backend, types.ProtoChainStream, true)
			return nil
		case errors.Is(err, errClientGone):
			// The client vanished: the outcome says nothing about the
			// backend. Release the admission instead of recording a failure
			// that would debit an innocent backend (the forwarder's
			// drainResults convention).
			d.circuit.Release(backend, types.ProtoChainStream)
			return nil
		case errors.Is(err, errUpstreamGone):
			// Resume: pick another candidate and replay, after a backoff.
			d.circuit.Record(backend, types.ProtoChainStream, false)
			metrics.SubscriptionResumes.WithLabelValues("upstream_close").Inc()
			lastFailed = backend
			delay = d.nextBackoff(delay)
			log.FromCtx(ctx).Info("chainstream: resuming after upstream loss",
				"backend", backend, "cursor", cursor, "attempts_left", attemptsLeft,
				"retry_in", delay.String(),
			)
			continue
		default:
			d.circuit.Record(backend, types.ProtoChainStream, false)
			lastErr = err
			lastFailed = backend
			delay = d.nextBackoff(delay)
			continue
		}
	}

	if lastErr == nil {
		lastErr = errors.New("chainstream: all attempts exhausted")
	}
	log.FromCtx(ctx).Error("chainstream: terminal error", "err", lastErr.Error())
	return status.Errorf(codes.Unavailable, "chainstream relay failed: %v", lastErr)
}

// runOnce performs one (open upstream → replay request → forward
// responses) cycle. Returns errClientGone, errUpstreamGone, or nil for
// clean upstream EOF. It never touches the circuit — handle owns the
// resolution of the admission pickConn claimed.
func (d *Director) runOnce(
	ctx context.Context,
	ss grpc.ServerStream,
	conn *grpc.ClientConn,
	method string,
	req RawMessage,
	cursor *uint64,
	backend string,
) error {
	// Forward incoming metadata to upstream verbatim, mark the chosen
	// backend in our log scope.
	md, _ := metadata.FromIncomingContext(ctx)
	outCtx := metadata.NewOutgoingContext(ctx, md.Copy())
	outCtx = metadata.AppendToOutgoingContext(outCtx, "x-stitch-request-id", runtime.NewRequestID())

	clientStream, err := conn.NewStream(outCtx, &grpc.StreamDesc{
		StreamName:    method,
		ServerStreams: true,
		ClientStreams: false,
	}, method, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return attributeErr(ctx)
	}

	if err := clientStream.SendMsg(&req); err != nil {
		return attributeErr(ctx)
	}
	if err := clientStream.CloseSend(); err != nil {
		return attributeErr(ctx)
	}

	// Forward server-streamed responses with cursor dedup.
	dropped, forwarded := 0, 0
	for {
		var resp RawMessage
		if err := clientStream.RecvMsg(&resp); err != nil {
			if errors.Is(err, io.EOF) {
				log.FromCtx(ctx).Debug("chainstream: upstream EOF",
					"backend", backend, "forwarded", forwarded, "dropped", dropped,
				)
				return nil
			}
			return attributeErr(ctx)
		}

		height, ok := subscription.ExtractStreamResponseHeight(resp.Bytes)
		if ok {
			if height <= *cursor && *cursor != 0 {
				dropped++
				continue
			}
			*cursor = height
		}

		if err := ss.SendMsg(&resp); err != nil {
			return errClientGone
		}
		forwarded++
	}
}

// pickConn returns the first candidate that is circuit-admitted and
// dialable, with its admission held: the caller resolves it with exactly
// one Record or Release. A dial failure is recorded right here — that
// candidate's admission is already resolved when the loop moves on.
//
// avoid names the backend whose stream failed last: it is deprioritized
// to last place for this pick, so a resume prefers a different upstream
// when one is viable but can still re-probe the failed one when it is the
// only option (single-backend sets, everyone else tripped). Without this
// a backend whose breaker cooldown is shorter than the resume backoff
// would win every re-pick and starve healthy alternatives.
func (d *Director) pickConn(ctx context.Context, avoid string) (string, *grpc.ClientConn) {
	candidates := d.selector.Candidates(types.RouteKey{
		Protocol: types.ProtoChainStream,
		Method:   "Stream",
		Class:    types.ClassSubscribe,
	})
	if avoid != "" {
		preferred := make([]*backend.Backend, 0, len(candidates))
		var avoided []*backend.Backend
		for _, b := range candidates {
			if b.Name == avoid {
				avoided = append(avoided, b)
			} else {
				preferred = append(preferred, b)
			}
		}
		candidates = append(preferred, avoided...)
	}
	for _, b := range candidates {
		ep := b.Endpoint(types.ProtoChainStream)
		if ep == "" {
			continue
		}
		// Re-check the breaker at commit time: it may have tripped between
		// selection and now. Acquire also claims the single half-open
		// canary slot; a skipped candidate claims nothing.
		if !d.circuit.Acquire(b.Name, types.ProtoChainStream) {
			continue
		}
		conn, err := d.pool.Conn(ctx, b.Name, pool.CleanAddr(ep))
		if err != nil {
			d.circuit.Record(b.Name, types.ProtoChainStream, false)
			continue
		}
		d.pool.Touch(b.Name)
		return b.Name, conn
	}
	return "", nil
}

// errClientGone, errUpstreamGone are sentinels exported only via the
// resume loop. If you see one bubbling out of handle(), that's a bug.
var (
	errClientGone   = errors.New("chainstream: client gone")
	errUpstreamGone = errors.New("chainstream: upstream gone")
)

// attributeErr classifies a failed upstream operation: when the
// server-stream ctx is already done, the client vanished and the failure
// indicts nobody (errClientGone → admission released); otherwise the
// upstream is at fault (errUpstreamGone → failure recorded, resume).
// Note: this deliberately differs from cosmos_grpc/handler.go — for a
// long-lived flowing stream, the client's deadline expiring is client
// policy (the stream lived as long as the client wanted), not backend
// failure, so ctx-done for any cause (Canceled or DeadlineExceeded)
// maps to client-gone here.
func attributeErr(ctx context.Context) error {
	if ctx.Err() != nil {
		return errClientGone
	}
	return errUpstreamGone
}

// nextBackoff doubles the previous delay (or starts at baseBackoff), then
// applies +/-20% jitter and clamps to [1ms, maxBackoff]. The 1ms floor
// prevents a hot spin if baseBackoff is somehow zero. Local copy of
// health.EthWSProber's nextBackoff (internal/health/probe_eth_ws.go) —
// the prober's is an unexported method, and an import would tie server
// code to the health package for seven lines of arithmetic.
func (d *Director) nextBackoff(prev time.Duration) time.Duration {
	next := prev * 2
	if next < d.baseBackoff {
		next = d.baseBackoff
	}
	if next > d.maxBackoff {
		next = d.maxBackoff
	}
	// +/- 20% jitter around `next`.
	span := int64(next) / 5
	var result time.Duration
	if span <= 0 {
		result = next
	} else {
		d.mu.Lock()
		j := d.rand.Int63n(2*span) - span
		d.mu.Unlock()
		result = next + time.Duration(j)
	}
	// Final clamp so jitter can't exceed maxBackoff or drop below 1ms.
	if d.maxBackoff > 0 && result > d.maxBackoff {
		result = d.maxBackoff
	}
	if result < time.Millisecond {
		result = time.Millisecond
	}
	return result
}

func isChainStreamMethod(method string) bool {
	method = strings.TrimPrefix(method, "/")
	for _, svc := range ChainStreamServiceNames {
		if strings.HasPrefix(method, svc+"/") {
			return true
		}
	}
	return false
}
