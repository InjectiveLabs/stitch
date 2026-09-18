package cosmos_grpc

import (
	"context"
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/circuit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// A retryable failure still has an ordinary gRPC response contract when no
// alternate can serve it: preserve its response headers as well as trailers.
func TestReviewHistoricalExhaustionPreservesResponseMetadata(t *testing.T) {
	r := setupOutcomeRig(t, circuit.Policy{ErrorThreshold: .5, MinRequests: 1, OpenDuration: time.Minute},
		func(_ string, stream grpc.ServerStream) error {
			if err := stream.SendHeader(metadata.Pairs("x-upstream-context", "retained", HeightHeader, "145000000")); err != nil {
				return err
			}
			stream.SetTrailer(metadata.Pairs("x-upstream-detail", "last shard"))
			return status.Error(codes.Unknown, "failed to load state; version does not exist")
		})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, "145000000")
	var header, trailer metadata.MD
	err := r.conn.Invoke(ctx, historicalMethod, &emptypb.Empty{}, &emptypb.Empty{}, grpc.Header(&header), grpc.Trailer(&trailer))
	r.waitFinished(t, historicalMethod)
	if status.Code(err) != codes.Unknown {
		t.Fatalf("status changed: %v", err)
	}
	if got := header.Get("x-upstream-context"); len(got) != 1 || got[0] != "retained" {
		t.Errorf("initial response metadata lost: %v", header)
	}
	if got := header.Get(HeightHeader); len(got) != 1 || got[0] != "145000000" {
		t.Errorf("response height lost: %v", header)
	}
	if got := trailer.Get("x-upstream-detail"); len(got) != 1 || got[0] != "last shard" {
		t.Errorf("final trailers lost: %v", trailer)
	}
}

func TestReviewHistoricalRetryOnlyExposesFinalMetadata(t *testing.T) {
	for _, succeed := range []bool{false, true} {
		name := "exhaustion"
		if succeed {
			name = "success"
		}
		t.Run(name, func(t *testing.T) {
			conn, _, _ := historicalRig(t,
				func(stream grpc.ServerStream, _ *emptypb.Empty) error {
					if err := stream.SendHeader(metadata.Pairs("x-first-attempt", "private", "x-backend", "first")); err != nil {
						return err
					}
					stream.SetTrailer(metadata.Pairs("x-first-trailer", "private"))
					return status.Error(codes.Unknown, historicalMissing)
				},
				func(stream grpc.ServerStream, _ *emptypb.Empty) error {
					if err := stream.SendHeader(metadata.Pairs("x-backend", "final")); err != nil {
						return err
					}
					stream.SetTrailer(metadata.Pairs("x-final-trailer", "kept"))
					if succeed {
						return stream.SendMsg(&emptypb.Empty{})
					}
					return status.Error(codes.Unknown, historicalMissing)
				})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, "105504992")
			var header, trailer metadata.MD
			err := conn.Invoke(ctx, historicalMethod, &emptypb.Empty{}, &emptypb.Empty{}, grpc.Header(&header), grpc.Trailer(&trailer))
			if succeed && err != nil {
				t.Fatal(err)
			}
			if !succeed && status.Code(err) != codes.Unknown {
				t.Fatal(err)
			}
			if got := header.Get("x-backend"); len(got) != 1 || got[0] != "final" {
				t.Errorf("final header lost: %v", header)
			}
			if len(header.Get("x-first-attempt")) != 0 || len(trailer.Get("x-first-trailer")) != 0 {
				t.Errorf("failed-attempt metadata leaked: %v %v", header, trailer)
			}
			if got := trailer.Get("x-final-trailer"); len(got) != 1 || got[0] != "kept" {
				t.Errorf("final trailer lost: %v", trailer)
			}
		})
	}
}

func TestReviewHistoricalMissingStateKeepsHalfOpenCircuitNeutral(t *testing.T) {
	r := setupOutcomeRig(t, circuit.Policy{ErrorThreshold: .5, MinRequests: 1, OpenDuration: time.Millisecond},
		func(_ string, _ grpc.ServerStream) error {
			return status.Error(codes.Unknown, "failed to load state; version does not exist")
		})
	r.circuit.Record(outcomeBackend, "grpc", false)
	time.Sleep(3 * time.Millisecond)
	if err := r.call(t, historicalMethod); status.Code(err) != codes.Unknown {
		t.Fatal(err)
	}
	if got := r.circuit.State(outcomeBackend, "grpc"); got != circuit.StateHalfOpen {
		t.Fatalf("retention error changed half-open state: %v", got)
	}
	if !r.circuit.Acquire(outcomeBackend, "grpc") {
		t.Fatal("retention error stranded half-open admission")
	}
	r.circuit.Release(outcomeBackend, "grpc")
}

// Unknown services under a familiar namespace may be bidirectional. Merely
// containing ".Query/" is not enough to infer a one-request read contract.
func TestReviewUnknownQueryServiceRetainsBidirectionalForwarding(t *testing.T) {
	conn, _, _ := historicalRig(t, func(stream grpc.ServerStream, _ *emptypb.Empty) error {
		if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
			return err
		}
		var second emptypb.Empty
		if err := stream.RecvMsg(&second); err != nil {
			return err
		}
		return stream.SendMsg(&second)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, "105504992")
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/cosmos.review.v1.Query/Watch")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	// A streaming peer need not half-close before reading its first response.
	if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
		t.Fatalf("first response blocked waiting for request EOF: %v", err)
	}
	if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
}
