package cosmos_grpc

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// A deadline may already have elapsed before its cancellation callback runs.
// Keeping Err nil makes that scheduling window deterministic in this test.
type delayedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c delayedDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }

type deadlineCandidates []*backend.Backend

func (bs deadlineCandidates) Candidates(types.RouteKey) []*backend.Backend { return bs }

func newDeadlineDirector(t *testing.T, bs ...*backend.Backend) *Director {
	t.Helper()
	gp := pool.NewGRPCPool(time.Minute)
	t.Cleanup(gp.CloseAll)
	cm := circuit.NewManager(circuit.Policy{MinRequests: 10, OpenDuration: time.Minute})
	return NewDirector(deadlineCandidates(bs), cm, gp)
}

func deadlineBackend(t *testing.T, name string, handler func(grpc.ServerStream) error) *backend.Backend {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		var request emptypb.Empty
		if err := stream.RecvMsg(&request); err != nil {
			return err
		}
		return handler(stream)
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return &backend.Backend{Name: name, Endpoints: map[types.Protocol]string{types.ProtoGRPC: lis.Addr().String()}}
}

func TestHistoricalElapsedDeadlineWithoutContextError(t *testing.T) {
	ctx := delayedDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Second)}
	_, err := newDeadlineDirector(t).replayHistorical(ctx, earliestParamsMethod, nil, metadata.MD{}, 50, nil)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("elapsed caller deadline became %v", err)
	}
}

func TestHistoricalDeadlineDuringLastAttemptPreservesCallerStatus(t *testing.T) {
	deadline := time.Now().Add(80 * time.Millisecond)
	ctx := delayedDeadlineContext{Context: context.Background(), deadline: deadline}
	slow := deadlineBackend(t, "slow", func(grpc.ServerStream) error {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		<-timer.C
		return status.Error(codes.DeadlineExceeded, "upstream request budget exhausted")
	})
	_, err := newDeadlineDirector(t, slow).replayHistorical(ctx, earliestParamsMethod, nil, metadata.MD{}, 50, nil)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("exhausted caller deadline became %v", err)
	}
}

func TestHistoricalBackendDeadlineStillRetriesSameHeight(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		waitForAttemptDeadline bool
	}{
		{name: "upstream_status"},
		{name: "attempt_timeout", waitForAttemptDeadline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var firstHits, secondHits atomic.Int64
			checkHeight := func(stream grpc.ServerStream) {
				md, _ := metadata.FromIncomingContext(stream.Context())
				if got := md.Get(HeightHeader); len(got) != 1 || got[0] != "50" {
					t.Errorf("retry changed requested height: %v", got)
				}
			}
			first := deadlineBackend(t, "first", func(stream grpc.ServerStream) error {
				firstHits.Add(1)
				checkHeight(stream)
				if tc.waitForAttemptDeadline {
					<-stream.Context().Done()
				}
				return status.Error(codes.DeadlineExceeded, "backend deadline expired")
			})
			second := deadlineBackend(t, "second", func(stream grpc.ServerStream) error {
				secondHits.Add(1)
				checkHeight(stream)
				if err := stream.SendHeader(metadata.Pairs(HeightHeader, "50")); err != nil {
					return err
				}
				return stream.SendMsg(&emptypb.Empty{})
			})
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			reply, err := newDeadlineDirector(t, first, second).replayHistorical(ctx, earliestParamsMethod, nil, metadata.MD{}, 50, nil)
			if err != nil || reply.err != nil {
				t.Fatalf("same-height failover lost caller budget: %v / %v", err, reply.err)
			}
			if firstHits.Load() != 1 || secondHits.Load() != 1 {
				t.Fatalf("unexpected attempts: first=%d second=%d", firstHits.Load(), secondHits.Load())
			}
			if ctx.Err() != nil {
				t.Fatalf("caller deadline expired before replica success: %v", ctx.Err())
			}
		})
	}
}
