package chainstream

import (
	"context"
	"errors"
	"io"
	"strings"

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
	"github.com/decentrio/stitch/internal/subscription"
	"github.com/decentrio/stitch/internal/types"
)

// Director runs one ChainStream call: receive the StreamRequest, then
// loop dial-and-relay across candidates, deduping by cursor when the
// upstream changes.
type Director struct {
	selector selector.Selector
	circuit  *circuit.Manager
	pool     *pool.GRPCPool
}

func NewDirector(s selector.Selector, c *circuit.Manager, p *pool.GRPCPool) *Director {
	return &Director{selector: s, circuit: c, pool: p}
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

	const maxAttempts = 8
	attemptsLeft := maxAttempts
	var lastErr error

	for attemptsLeft > 0 {
		attemptsLeft--

		backend, conn := d.pickConn(ctx)
		if conn == nil {
			lastErr = errors.New("no eligible backend")
			break
		}

		err := d.runOnce(ctx, ss, conn, method, req, &cursor, backend)
		switch {
		case err == nil:
			// Upstream finished cleanly (server-side EOF). Pass through.
			return nil
		case errors.Is(err, errClientGone):
			return nil
		case errors.Is(err, errUpstreamGone):
			// Resume: pick another candidate and replay.
			d.circuit.Record(backend, types.ProtoChainStream, false)
			metrics.SubscriptionResumes.WithLabelValues("upstream_close").Inc()
			log.FromCtx(ctx).Info("chainstream: resuming after upstream loss",
				"backend", backend, "cursor", cursor, "attempts_left", attemptsLeft,
			)
			continue
		default:
			d.circuit.Record(backend, types.ProtoChainStream, false)
			lastErr = err
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
// clean upstream EOF.
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
		return errUpstreamGone
	}

	if err := clientStream.SendMsg(&req); err != nil {
		return errUpstreamGone
	}
	if err := clientStream.CloseSend(); err != nil {
		return errUpstreamGone
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
				d.circuit.Record(backend, types.ProtoChainStream, true)
				return nil
			}
			return errUpstreamGone
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

func (d *Director) pickConn(ctx context.Context) (string, *grpc.ClientConn) {
	candidates := d.selector.Candidates(types.RouteKey{
		Protocol: types.ProtoChainStream,
		Method:   "Stream",
		Class:    types.ClassSubscribe,
	})
	for _, b := range candidates {
		if !d.circuit.Allow(b.Name, types.ProtoChainStream) {
			continue
		}
		ep := b.Endpoint(types.ProtoChainStream)
		if ep == "" {
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

func isChainStreamMethod(method string) bool {
	method = strings.TrimPrefix(method, "/")
	for _, svc := range ChainStreamServiceNames {
		if strings.HasPrefix(method, svc+"/") {
			return true
		}
	}
	return false
}
