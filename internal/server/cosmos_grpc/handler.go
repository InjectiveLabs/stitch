package cosmos_grpc

import (
	"context"
	"errors"
	"time"

	"github.com/mwitkow/grpc-proxy/proxy"
	"google.golang.org/grpc"
)

// streamHandler wraps proxy.TransparentHandler so we can:
//  1. Install a "chosen backend" slot in the server-stream context
//     before the director runs.
//  2. Read it after the proxy returns, and resolve the circuit admission
//     claimed in Direct with the actual RPC outcome.
//
// Without this, only dial failures trip the breaker; an upstream that
// successfully accepts the connection but errors on every call would never
// be circuit-protected. A set slot means Direct committed to a backend and
// holds an admission, so exactly one resolution — Record or Release —
// happens here.
func streamHandler(dir *Director) grpc.StreamHandler {
	inner := proxy.TransparentHandler(dir.Direct)
	return func(srv any, ss grpc.ServerStream) error {
		slot := &atomicString{}
		wrapped := &slotStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), chosenBackendKey, slot)}
		err := inner(srv, wrapped)
		if name := slot.Get(); name != "" {
			switch {
			case err == nil:
				dir.RecordOutcome(name, true)
			case errors.Is(ss.Context().Err(), context.Canceled) && !deadlineExpired(ss.Context()):
				// Convention (mirrors forwarder/broadcast.go drainResults):
				//   cancellation  = client walked away, neutral Release;
				//   deadline expiry = backend too slow, recorded failure.
				// Some grpc-go paths surface an expired client deadline as
				// context.Canceled on the server stream, so check the actual
				// deadline before deciding this is neutral.
				dir.ReleaseOutcome(name)
			default:
				dir.RecordOutcome(name, false)
			}
		}
		return err
	}
}

func deadlineExpired(ctx context.Context) bool {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	deadline, ok := ctx.Deadline()
	return ok && !time.Now().Before(deadline)
}

// slotStream overrides Context() so the director sees the slot. All other
// grpc.ServerStream methods pass through to the underlying stream.
type slotStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *slotStream) Context() context.Context { return s.ctx }
