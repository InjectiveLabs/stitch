package cosmos_grpc

import (
	"context"
	"errors"
	"io"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/InjectiveLabs/stitch/internal/history"
	"github.com/InjectiveLabs/stitch/internal/log"
	"github.com/InjectiveLabs/stitch/internal/metrics"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/types"
)

func (d *Director) forwardHistorical(ss *slotStream, method string, key types.RouteKey) (result error) {
	started := time.Now()
	lastBackend := ""
	defer func() {
		if lastBackend == "" {
			return
		}
		outcome := classifyRPCOutcome(ss.Context(), result)
		log.FromCtx(ss.Context()).Debug("grpc: RPC outcome", "backend", lastBackend,
			"protocol", string(types.ProtoGRPC), "method", method, "class", key.Class.String(),
			"height", key.HeightOrZero(), "grpc_status", status.Code(result).String(),
			"outcome", string(outcome), "duration_ms", time.Since(started).Milliseconds())
	}()
	var request, extra emptypb.Empty
	if err := ss.RecvMsg(&request); err != nil {
		return err
	}
	if err := ss.RecvMsg(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return status.Error(codes.InvalidArgument, "historical query requires one request message")
	}
	md, _ := metadata.FromIncomingContext(ss.Context())
	md = md.Copy()
	if _, hasHeight := metadataHeight(md); !hasHeight {
		md.Set(HeightHeader, strconv.FormatInt(key.HeightOrZero(), 10))
	}
	md.Set("x-stitch-request-id", log.RequestID(ss.Context()))
	var lastErr error
	var lastTrailer metadata.MD
	var lastHeader metadata.MD
	attempts := 0
	for _, b := range d.selector.Candidates(key) {
		if err := ss.Context().Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		if attempts >= d.maxAttempts {
			break
		}
		ep := b.Endpoint(types.ProtoGRPC)
		if ep == "" || !d.circuit.Acquire(b.Name, types.ProtoGRPC) {
			continue
		}
		attempts++
		lastBackend = b.Name
		metrics.RequestsTotal.WithLabelValues(string(types.ProtoGRPC), key.Class.String(), b.Name, "directed").Inc()
		attemptStarted := time.Now()
		ctx, cancel := context.WithTimeout(ss.Context(), d.perAttemptTimeout)
		ctx = metadata.NewOutgoingContext(ctx, md)
		conn, err := d.pool.Conn(ctx, b.Name, pool.CleanAddr(ep))
		var upstream grpc.ClientStream
		if err == nil {
			upstream, err = conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, method)
		}
		if err == nil {
			err = upstream.SendMsg(&request)
		}
		if err == nil {
			err = upstream.CloseSend()
		}
		var first emptypb.Empty
		if err == nil {
			err = upstream.RecvMsg(&first)
		}
		metrics.BackendLatency.WithLabelValues(b.Name, string(types.ProtoGRPC)).Observe(time.Since(attemptStarted).Seconds())
		if err != nil && !errors.Is(err, io.EOF) {
			lastErr = err
			lastTrailer = nil
			lastHeader = nil
			if upstream != nil {
				lastTrailer = upstream.Trailer()
				lastHeader, _ = upstream.Header()
			}
			missing := missingStateStatus(err)
			outcome := classifyRPCOutcome(ss.Context(), err)
			if missing || outcome == rpcNeutral {
				d.ReleaseOutcome(b.Name)
			} else {
				d.RecordOutcome(b.Name, false)
			}
			cancel()
			if ss.Context().Err() != nil {
				return status.FromContextError(ss.Context().Err()).Err()
			}
			if missing || status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded {
				reason := "availability"
				if missing {
					reason = "missing_state"
				}
				metrics.FailoverAttempts.WithLabelValues(b.Name, "next", reason).Inc()
				continue
			}
			ss.SetTrailer(lastTrailer)
			if len(lastHeader) > 0 {
				_ = ss.SendHeader(lastHeader)
			}
			return err
		}
		// The first message commits this response. Never replay after this
		// point, even if an upstream stream later fails with a retention error.
		header, headerErr := upstream.Header()
		if headerErr == nil {
			headerErr = ss.SendHeader(header)
		}
		if headerErr != nil {
			d.ReleaseOutcome(b.Name)
			cancel()
			return headerErr
		}
		for err == nil {
			if sendErr := ss.SendMsg(&first); sendErr != nil {
				d.ReleaseOutcome(b.Name)
				cancel()
				return sendErr
			}
			first.Reset()
			err = upstream.RecvMsg(&first)
		}
		ss.SetTrailer(upstream.Trailer())
		if errors.Is(err, io.EOF) {
			err = nil
		}
		switch classifyRPCOutcome(ss.Context(), err) {
		case rpcSuccess:
			d.RecordOutcome(b.Name, true)
		case rpcFailure:
			d.RecordOutcome(b.Name, false)
		default:
			d.ReleaseOutcome(b.Name)
		}
		cancel()
		return err
	}
	if lastErr == nil {
		lastErr = status.Error(codes.Unavailable, "no eligible backend for historical query")
	}
	ss.SetTrailer(lastTrailer)
	if len(lastHeader) > 0 {
		_ = ss.SendHeader(lastHeader)
	}
	return lastErr
}

func missingStateStatus(err error) bool {
	switch status.Code(err) {
	case codes.Unknown, codes.Internal, codes.NotFound, codes.FailedPrecondition, codes.OutOfRange:
		return history.Unavailable(status.Convert(err).Message())
	default:
		return false
	}
}
