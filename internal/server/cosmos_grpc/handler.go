package cosmos_grpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/mwitkow/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
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
		_, hadDeadline := ss.Context().Deadline()
		ctx := context.WithValue(ss.Context(), chosenBackendKey, slot)
		wrapped := &slotStream{ServerStream: ss, ctx: ctx}

		// The proxy director normally runs before RecvMsg, so it cannot see
		// request fields. For manifest-declared height queries, receive the
		// first frame now, extract its routing height, and replay the exact
		// protobuf payload when mwitkow starts its normal forwarding loop.
		if method, ok := grpc.MethodFromServerStream(ss); ok {
			md, _ := metadata.FromIncomingContext(ss.Context())
			_, hasMetadataHeight := metadataHeight(md)
			if _, bodyRoutable := Lookup(method); bodyRoutable && !hasMetadataHeight {
				var first emptypb.Empty
				if err := ss.RecvMsg(&first); err != nil {
					return err
				}
				payload := append([]byte(nil), first.ProtoReflect().GetUnknown()...)
				wrapped.first = payload
				wrapped.hasFirst = true
				if height, found := extractRequestHeight(method, payload); found {
					wrapped.ctx = context.WithValue(ctx, requestHeightKey, height)
				}
			}
		}
		err := inner(srv, wrapped)
		if name := slot.Get(); name != "" {
			switch {
			case err == nil:
				dir.RecordOutcome(name, true)
			case errors.Is(ss.Context().Err(), context.Canceled) && !hadDeadline:
				// Convention (mirrors forwarder/broadcast.go drainResults):
				//   cancellation  = client walked away, neutral Release;
				//   deadline expiry = backend too slow, recorded failure.
				// Some grpc-go paths surface a client deadline as
				// context.Canceled on the server stream, so a request with a
				// caller-supplied deadline is treated as part of the backend's
				// service budget rather than a neutral disconnect.
				dir.ReleaseOutcome(name)
			default:
				dir.RecordOutcome(name, false)
			}
		}
		return err
	}
}

// slotStream exposes per-call routing state through Context. For body-routed
// methods it also replays the first request frame that streamHandler consumed
// before invoking the director; subsequent frames pass through unchanged.
type slotStream struct {
	grpc.ServerStream
	ctx      context.Context
	first    []byte
	hasFirst bool
}

func (s *slotStream) Context() context.Context { return s.ctx }

func (s *slotStream) RecvMsg(dst any) error {
	if !s.hasFirst {
		return s.ServerStream.RecvMsg(dst)
	}
	s.hasFirst = false
	payload := s.first
	s.first = nil
	msg, ok := dst.(proto.Message)
	if !ok {
		return fmt.Errorf("cosmos_grpc: cannot replay first request into %T", dst)
	}
	return proto.Unmarshal(payload, msg)
}
