package cosmos_grpc

import (
	"context"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/types"
)

const historicalMissing = "failed to load state at height 105504992; version mismatch on immutable IAVL tree; version does not exist"
const historicalMethod = "/cosmos.bank.v1beta1.Query/Params"

type orderedHistoricalSelector struct{ backends []*backend.Backend }

func (s orderedHistoricalSelector) Candidates(types.RouteKey) []*backend.Backend { return s.backends }

func historicalRig(t *testing.T, handlers ...func(grpc.ServerStream, *emptypb.Empty) error) (*grpc.ClientConn, *circuit.Manager, *Director) {
	t.Helper()
	var backends []*backend.Backend
	for i, h := range handlers {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, ss grpc.ServerStream) error {
			var req emptypb.Empty
			if err := ss.RecvMsg(&req); err != nil {
				return err
			}
			return h(ss, &req)
		}))
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(srv.Stop)
		backends = append(backends, &backend.Backend{Name: string(rune('a' + i)), Endpoints: map[types.Protocol]string{types.ProtoGRPC: lis.Addr().String()}})
	}
	cm := circuit.NewManager(circuit.Policy{MinRequests: 1, ErrorThreshold: .5, OpenDuration: time.Minute})
	p := pool.NewGRPCPool(time.Minute)
	t.Cleanup(p.CloseAll)
	d := NewDirector(orderedHistoricalSelector{backends}, cm, p)
	front, err := New("127.0.0.1:0", d)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = front.Start(context.Background()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = front.Shutdown(ctx)
	})
	conn, err := grpc.NewClient(front.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, cm, d
}

func TestGRPCHistoricalRetryPreservesRequestAndFinalMetadata(t *testing.T) {
	var hits [2]atomic.Int32
	want := wrapperspb.String("opaque query payload")
	check := func(ss grpc.ServerStream, req *emptypb.Empty) {
		md, _ := metadata.FromIncomingContext(ss.Context())
		if strings.Join(md.Get(HeightHeader), "") != "105504992" {
			t.Error("historical height lost")
		}
		if strings.Join(md.Get("authorization"), "") != "test-token" {
			t.Error("metadata lost")
		}
		data, _ := proto.Marshal(want)
		if string(req.ProtoReflect().GetUnknown()) != string(data) {
			t.Error("protobuf payload changed")
		}
	}
	conn, cm, _ := historicalRig(t, func(ss grpc.ServerStream, req *emptypb.Empty) error {
		hits[0].Add(1)
		check(ss, req)
		ss.SetTrailer(metadata.Pairs("attempt", "missing"))
		return status.Error(codes.Unknown, historicalMissing)
	}, func(ss grpc.ServerStream, req *emptypb.Empty) error {
		hits[1].Add(1)
		check(ss, req)
		_ = ss.SetHeader(metadata.Pairs(HeightHeader, "105504992"))
		ss.SetTrailer(metadata.Pairs("attempt", "success"))
		return ss.SendMsg(wrapperspb.String("historical value"))
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, "105504992", "authorization", "test-token")
	var got wrapperspb.StringValue
	var header, trailer metadata.MD
	if err := conn.Invoke(ctx, historicalMethod, want, &got, grpc.Header(&header), grpc.Trailer(&trailer)); err != nil {
		t.Fatal(err)
	}
	if got.Value != "historical value" || hits[0].Load() != 1 || hits[1].Load() != 1 {
		t.Fatalf("response=%q hits=%d,%d", got.Value, hits[0].Load(), hits[1].Load())
	}
	if strings.Join(header.Get(HeightHeader), "") != "105504992" || strings.Join(trailer.Get("attempt"), "") != "success" {
		t.Fatalf("metadata leaked or lost: %v %v", header, trailer)
	}
	if cm.State("a", types.ProtoGRPC) != circuit.StateClosed {
		t.Fatal("retention failure opened circuit")
	}
}

func TestGRPCHistoricalRetryLimitsAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name, method string
		header       bool
		code         codes.Code
		message      string
		max          int
		wantHits     int
	}{
		{"all_missing", historicalMethod, true, codes.Unknown, historicalMissing, 3, 2},
		{"one_attempt", historicalMethod, true, codes.Unknown, historicalMissing, 1, 1},
		{"ordinary_error", historicalMethod, true, codes.Internal, "permission denied", 3, 1},
		{"permission_status", historicalMethod, true, codes.PermissionDenied, historicalMissing, 3, 1},
		{"latest", historicalMethod, false, codes.Unknown, historicalMissing, 3, 1},
		{"broadcast", "/cosmos.tx.v1beta1.Service/BroadcastTx", true, codes.Unknown, historicalMissing, 3, 1},
		{"unknown_method", "/custom.Service/Write", true, codes.Unknown, historicalMissing, 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			h := func(ss grpc.ServerStream, _ *emptypb.Empty) error {
				hits.Add(1)
				ss.SetTrailer(metadata.Pairs("attempt", "final"))
				return status.Error(tc.code, tc.message)
			}
			conn, _, dir := historicalRig(t, h, h)
			dir.SetFailoverPolicy(tc.max, time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if tc.header {
				ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, "105504992")
			}
			var trailer metadata.MD
			err := conn.Invoke(ctx, tc.method, &emptypb.Empty{}, &emptypb.Empty{}, grpc.Trailer(&trailer))
			if status.Code(err) != tc.code || status.Convert(err).Message() != tc.message || int(hits.Load()) != tc.wantHits {
				t.Fatalf("error=%v hits=%d", err, hits.Load())
			}
			if strings.Join(trailer.Get("attempt"), "") != "final" {
				t.Fatalf("trailers=%v", trailer)
			}
		})
	}
}

func TestGRPCHistoricalNeverRetriesAfterResponse(t *testing.T) {
	var fallback atomic.Int32
	conn, _, _ := historicalRig(t, func(ss grpc.ServerStream, _ *emptypb.Empty) error {
		if err := ss.SendMsg(&emptypb.Empty{}); err != nil {
			return err
		}
		return status.Error(codes.Unknown, historicalMissing)
	}, func(ss grpc.ServerStream, _ *emptypb.Empty) error {
		fallback.Add(1)
		return ss.SendMsg(&emptypb.Empty{})
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, "105504992")
	s, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, historicalMethod)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SendMsg(&emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	if err = s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if err = s.RecvMsg(&emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	if err = s.RecvMsg(&emptypb.Empty{}); status.Code(err) != codes.Unknown {
		t.Fatalf("got %v", err)
	}
	if fallback.Load() != 0 {
		t.Fatal("partial response retried")
	}
}

func TestGRPCHistoricalBodyHeightFallback(t *testing.T) {
	var first atomic.Int32
	conn, _, _ := historicalRig(t, func(_ grpc.ServerStream, _ *emptypb.Empty) error {
		first.Add(1)
		return status.Error(codes.Unknown, historicalMissing)
	}, func(ss grpc.ServerStream, _ *emptypb.Empty) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		if strings.Join(md.Get(HeightHeader), "") != "105504992" {
			t.Error("body height not forwarded")
		}
		return ss.SendMsg(&emptypb.Empty{})
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := &emptypb.Empty{}
	req.ProtoReflect().SetUnknown(heightPayload(1, 105504992))
	if err := conn.Invoke(ctx, "/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight", req, &emptypb.Empty{}); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if first.Load() != 1 {
		t.Fatalf("first hits=%d", first.Load())
	}
}
