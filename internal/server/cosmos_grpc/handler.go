package cosmos_grpc

import (
	"context"
	"errors"

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
			case errors.Is(ss.Context().Err(), context.Canceled):
				// Convention (mirrors forwarder/broadcast.go drainResults):
				//   cancellation  = client walked away, neutral Release;
				//   deadline expiry = backend too slow, recorded failure.
				// A DeadlineExceeded context means the backend hung until the
				// client's gRPC deadline fired — that should be debited, so it
				// falls through to RecordOutcome(name, false) below.
				dir.ReleaseOutcome(name)
			default:
				dir.RecordOutcome(name, false)
			}
		}
		return err
	}
}

// slotStream overrides Context() so the director sees the slot. All other
// grpc.ServerStream methods pass through to the underlying stream.
type slotStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *slotStream) Context() context.Context { return s.ctx }
