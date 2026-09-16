package cosmos_grpc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/mwitkow/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/InjectiveLabs/stitch/internal/log"
	"github.com/InjectiveLabs/stitch/internal/runtime"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// streamHandler wraps proxy.TransparentHandler so we can:
//  1. Install a "chosen backend" slot in the server-stream context
//     before the director runs.
//  2. Read it after the proxy returns, and resolve the circuit admission
//     claimed in Direct with the actual RPC outcome.
//
// Without this, only dial failures trip the breaker; an upstream that
// successfully accepts the connection but returns availability failures
// would never be circuit-protected. Application status errors are neutral
// because the breaker is shared by every method on that backend. A set slot
// means Direct committed to a backend and holds an admission, so exactly
// one resolution — Record or Release —
// happens here.
func streamHandler(dir *Director) grpc.StreamHandler {
	inner := proxy.TransparentHandler(dir.Direct)
	return func(srv any, ss grpc.ServerStream) error {
		started := time.Now()
		slot := &atomicString{}
		ctx := log.WithRequestID(ss.Context(), runtime.NewRequestID())
		ctx = context.WithValue(ctx, chosenBackendKey, slot)
		wrapped := &slotStream{ServerStream: ss, ctx: ctx}
		method, hasMethod := grpc.MethodFromServerStream(ss)

		// The proxy director normally runs before RecvMsg, so it cannot see
		// request fields. For manifest-declared height queries, receive the
		// first frame now, extract its routing height, and replay the exact
		// protobuf payload when mwitkow starts its normal forwarding loop.
		if hasMethod {
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
			outcome := classifyRPCOutcome(ss.Context(), err)
			switch outcome {
			case rpcSuccess:
				dir.RecordOutcome(name, true)
			case rpcFailure:
				dir.RecordOutcome(name, false)
			case rpcNeutral:
				dir.ReleaseOutcome(name)
			}
			if log.L().Enabled(wrapped.ctx, slog.LevelDebug) {
				md, _ := metadata.FromIncomingContext(wrapped.ctx)
				key := buildRouteKey(method, md, requestHeight(wrapped.ctx))
				log.FromCtx(wrapped.ctx).Debug("grpc: RPC outcome",
					"backend", name,
					"protocol", string(types.ProtoGRPC),
					"method", method,
					"class", key.Class.String(),
					"height", key.HeightOrZero(),
					"grpc_status", status.Code(err).String(),
					"outcome", string(outcome),
					"duration_ms", time.Since(started).Milliseconds(),
				)
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
