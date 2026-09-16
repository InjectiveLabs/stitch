package cosmos_grpc

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"strings"
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
	healthreg "github.com/InjectiveLabs/stitch/internal/health"
	stitchlog "github.com/InjectiveLabs/stitch/internal/log"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
)

const (
	outcomeBackend = "historical-shard"
	traceMethod    = "/injective.evm.v1.Query/TraceTx"
	balanceMethod  = "/injective.evm.v1.Query/Balance"
	traceDenied    = "failed to create new contract: EVM Create operation is not authorized for user"
)

type outcomeRig struct {
	conn     *grpc.ClientConn
	upstream *grpc.Server
	circuit  *circuit.Manager
	finished chan string
}

// Use real gRPC connections on both sides of the production stream handler.
// The completion channel waits for outcome recording even when a client's
// canceled Invoke returns before the proxy has finished handling its stream.
func setupOutcomeRig(t *testing.T, policy circuit.Policy, handler func(string, grpc.ServerStream) error) *outcomeRig {
	t.Helper()
	listen := func() net.Listener {
		t.Helper()
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		return lis
	}
	upstreamListener := listen()
	upstream := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		var request emptypb.Empty
		if err := stream.RecvMsg(&request); err != nil {
			return err
		}
		method, _ := grpc.MethodFromServerStream(stream)
		return handler(method, stream)
	}))
	go func() { _ = upstream.Serve(upstreamListener) }()
	t.Cleanup(upstream.Stop)
	reg := backend.NewRegistry([]*backend.Backend{{
		Name: outcomeBackend, Weight: 100,
		Coverage:  backend.Coverage{Kind: backend.CovBounded, Lower: 138171000, Upper: 152684027},
		Endpoints: map[types.Protocol]string{types.ProtoGRPC: upstreamListener.Addr().String()},
	}})
	health := healthreg.NewRegistry()
	health.Update(healthreg.Snapshot{Backend: outcomeBackend, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 152684027})
	cm := circuit.NewManager(policy)
	connections := pool.NewGRPCPool(time.Minute)
	t.Cleanup(connections.CloseAll)
	dir := NewDirector(selector.NewRangeSelector(reg, health, cm, 0), cm, connections)
	rig := &outcomeRig{upstream: upstream, circuit: cm, finished: make(chan string, 32)}
	productionHandler := streamHandler(dir)
	front := grpc.NewServer(grpc.UnknownServiceHandler(func(server any, stream grpc.ServerStream) error {
		err := productionHandler(server, stream)
		method, _ := grpc.MethodFromServerStream(stream)
		rig.finished <- method
		return err
	}))
	frontListener := listen()
	go func() { _ = front.Serve(frontListener) }()
	t.Cleanup(front.Stop)
	conn, err := grpc.NewClient(frontListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	rig.conn = conn
	t.Cleanup(func() { _ = conn.Close() })
	return rig
}

func (r *outcomeRig) waitFinished(t *testing.T, method string) {
	t.Helper()
	select {
	case got := <-r.finished:
		if got != method {
			t.Fatalf("completed method %q; want %q", got, method)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("proxy did not finish %s", method)
	}
}

func (r *outcomeRig) call(t *testing.T, method string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, "145000000")
	err := r.conn.Invoke(ctx, method, &emptypb.Empty{}, &emptypb.Empty{})
	r.waitFinished(t, method)
	return err
}

func TestGRPCTraceApplicationErrorsDoNotBlockBalance(t *testing.T) {
	want, err := status.New(codes.Internal, traceDenied).WithDetails(wrapperspb.String("opaque upstream detail"))
	if err != nil {
		t.Fatal(err)
	}
	r := setupOutcomeRig(t, circuit.Policy{ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: time.Minute},
		func(method string, stream grpc.ServerStream) error {
			if method == traceMethod {
				return want.Err()
			}
			return stream.SendMsg(&emptypb.Empty{})
		})
	for i := 0; i < 6; i++ {
		got := status.Convert(r.call(t, traceMethod))
		if !proto.Equal(got.Proto(), want.Proto()) {
			t.Errorf("trace call %d changed upstream status: got %v; want %v", i, got, want)
		}
	}
	if got := r.circuit.State(outcomeBackend, types.ProtoGRPC); got != circuit.StateClosed {
		t.Errorf("application errors tripped the shared gRPC circuit: %s", got)
	}
	if err := r.call(t, balanceMethod); err != nil {
		t.Errorf("Balance on the healthy backend was blocked after trace errors: %v", err)
	}
}

func TestGRPCEarlyCancellationWithDeadlineIsNeutral(t *testing.T) {
	started := make(chan struct{})
	r := setupOutcomeRig(t, circuit.Policy{ErrorThreshold: 0.5, MinRequests: 1, OpenDuration: time.Minute},
		func(method string, stream grpc.ServerStream) error {
			if method == traceMethod {
				close(started)
				<-stream.Context().Done()
				return stream.Context().Err()
			}
			return stream.SendMsg(&emptypb.Empty{})
		})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, "145000000")
	result := make(chan error, 1)
	go func() { result <- r.conn.Invoke(ctx, traceMethod, &emptypb.Empty{}, &emptypb.Empty{}) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not reach upstream")
	}
	cancel() // The caller leaves well before its supplied deadline.
	select {
	case err := <-result:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("early cancellation returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled request did not return")
	}
	r.waitFinished(t, traceMethod)
	if got := r.circuit.State(outcomeBackend, types.ProtoGRPC); got != circuit.StateClosed {
		t.Errorf("early cancellation with a future deadline tripped circuit: %s", got)
	}
	if err := r.call(t, balanceMethod); err != nil {
		t.Errorf("Balance blocked after early client cancellation: %v", err)
	}
}

func TestGRPCApplicationStatusesRemainUsable(t *testing.T) {
	for _, code := range []codes.Code{
		codes.Canceled, codes.Unknown, codes.InvalidArgument, codes.NotFound,
		codes.AlreadyExists, codes.PermissionDenied, codes.ResourceExhausted,
		codes.FailedPrecondition, codes.Aborted, codes.OutOfRange,
		codes.Unimplemented, codes.Internal, codes.DataLoss, codes.Unauthenticated,
	} {
		t.Run(code.String(), func(t *testing.T) {
			want := status.New(code, "application rejected this request")
			r := setupOutcomeRig(t, circuit.Policy{ErrorThreshold: 0.5, MinRequests: 1, OpenDuration: time.Minute},
				func(method string, stream grpc.ServerStream) error {
					if method == traceMethod {
						return want.Err()
					}
					return stream.SendMsg(&emptypb.Empty{})
				})
			if got := status.Convert(r.call(t, traceMethod)); !proto.Equal(got.Proto(), want.Proto()) {
				t.Fatalf("upstream status changed: got %v; want %v", got, want)
			}
			if got := r.circuit.State(outcomeBackend, types.ProtoGRPC); got != circuit.StateClosed {
				t.Fatalf("application status %s tripped circuit: %s", code, got)
			}
			if err := r.call(t, balanceMethod); err != nil {
				t.Fatalf("Balance blocked by unrelated %s response: %v", code, err)
			}
		})
	}
}

func TestGRPCFailureStatusesStillTripCircuit(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			r := setupOutcomeRig(t, circuit.Policy{ErrorThreshold: 0.5, MinRequests: 1, OpenDuration: time.Minute},
				func(_ string, _ grpc.ServerStream) error {
					return status.Error(code, "backend unavailable or too slow")
				})
			if err := r.call(t, traceMethod); status.Code(err) != code {
				t.Fatalf("upstream failure changed: %v", err)
			}
			if got := r.circuit.State(outcomeBackend, types.ProtoGRPC); got != circuit.StateOpen {
				t.Fatalf("genuine %s failure did not trip circuit: %s", code, got)
			}
			if err := r.call(t, balanceMethod); status.Code(err) != codes.Unavailable {
				t.Fatalf("open circuit admitted Balance: %v", err)
			}
		})
	}
}

func TestGRPCNeutralHalfOpenTraceReleasesCanary(t *testing.T) {
	r := setupOutcomeRig(t, circuit.Policy{ErrorThreshold: 0.5, MinRequests: 1, OpenDuration: time.Millisecond},
		func(method string, stream grpc.ServerStream) error {
			switch method {
			case "/test.Backend/Unavailable":
				return status.Error(codes.Unavailable, "backend temporarily unreachable")
			case traceMethod:
				return status.Error(codes.Internal, traceDenied)
			default:
				return stream.SendMsg(&emptypb.Empty{})
			}
		})
	if err := r.call(t, "/test.Backend/Unavailable"); status.Code(err) != codes.Unavailable {
		t.Fatalf("initial backend failure: %v", err)
	}
	if got := r.circuit.State(outcomeBackend, types.ProtoGRPC); got != circuit.StateOpen {
		t.Fatalf("initial backend failure did not open circuit: %s", got)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !r.circuit.Allow(outcomeBackend, types.ProtoGRPC) {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("circuit cooldown did not elapse")
		}
	}
	for i := 0; i < 2; i++ {
		if err := r.call(t, traceMethod); status.Code(err) != codes.Internal || status.Convert(err).Message() != traceDenied {
			t.Fatalf("neutral canary %d changed trace error: %v", i, err)
		}
		if got := r.circuit.State(outcomeBackend, types.ProtoGRPC); got != circuit.StateHalfOpen {
			t.Fatalf("neutral canary resolved the circuit instead of releasing it: %s", got)
		}
		if !r.circuit.Allow(outcomeBackend, types.ProtoGRPC) {
			t.Fatal("neutral trace outcome leaked the half-open canary slot")
		}
	}
	if err := r.call(t, balanceMethod); err != nil {
		t.Fatalf("Balance could not acquire the released canary slot: %v", err)
	}
	if got := r.circuit.State(outcomeBackend, types.ProtoGRPC); got != circuit.StateClosed {
		t.Fatalf("successful Balance did not close circuit: %s", got)
	}
}

func TestGRPCTransportDisconnectStillTripsCircuit(t *testing.T) {
	started := make(chan struct{})
	r := setupOutcomeRig(t, circuit.Policy{ErrorThreshold: 0.5, MinRequests: 1, OpenDuration: time.Minute},
		func(_ string, stream grpc.ServerStream) error {
			close(started)
			<-stream.Context().Done()
			return stream.Context().Err()
		})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, "145000000")
	result := make(chan error, 1)
	go func() { result <- r.conn.Invoke(ctx, traceMethod, &emptypb.Empty{}, &emptypb.Empty{}) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("request did not reach upstream")
	}
	r.upstream.Stop() // Tear down the actual upstream transport mid-call.
	select {
	case err := <-result:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("transport disconnect returned %v; want Unavailable", err)
		}
	case <-ctx.Done():
		t.Fatal("transport disconnect did not reach caller")
	}
	r.waitFinished(t, traceMethod)
	if got := r.circuit.State(outcomeBackend, types.ProtoGRPC); got != circuit.StateOpen {
		t.Fatalf("transport disconnect did not trip circuit: %s", got)
	}
}

type canceledDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c canceledDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }

func TestGRPCCallerContextTakesPrecedence(t *testing.T) {
	early, cancelEarly := context.WithTimeout(context.Background(), time.Minute)
	cancelEarly()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want rpcOutcome
	}{
		{"early cancel reported as unavailable", early, status.Error(codes.Unavailable, "client disconnected"), rpcNeutral},
		{"elapsed deadline reported as canceled", expired, status.Error(codes.Canceled, "request canceled"), rpcFailure},
		{"grpc canceled context after deadline", canceledDeadlineContext{Context: early, deadline: time.Now().Add(-time.Second)}, status.Error(codes.Canceled, "request canceled"), rpcFailure},
		{"successful response before cancellation observed", early, nil, rpcSuccess},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyRPCOutcome(tc.ctx, tc.err); got != tc.want {
				t.Errorf("outcome=%v; want %v", got, tc.want)
			}
		})
	}
}

func TestGRPCOutcomeLogCorrelatesWithoutRequestData(t *testing.T) {
	var logs bytes.Buffer
	if err := stitchlog.Init("debug", "json", &logs); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stitchlog.Init("info", "text", os.Stderr) })
	upstreamID := make(chan string, 1)
	want, err := status.New(codes.Internal, "private-error-message: "+traceDenied).WithDetails(wrapperspb.String("private-response-detail"))
	if err != nil {
		t.Fatal(err)
	}
	r := setupOutcomeRig(t, circuit.Policy{ErrorThreshold: 0.5, MinRequests: 1, OpenDuration: time.Minute},
		func(_ string, stream grpc.ServerStream) error {
			md, _ := metadata.FromIncomingContext(stream.Context())
			upstreamID <- strings.Join(md.Get("x-stitch-request-id"), "")
			return want.Err()
		})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, "145000000", "authorization", "private-metadata-value")
	payload, err := proto.Marshal(wrapperspb.String("private-request-body"))
	if err != nil {
		t.Fatal(err)
	}
	request := &emptypb.Empty{}
	request.ProtoReflect().SetUnknown(payload)
	callErr := r.conn.Invoke(ctx, traceMethod, request, &emptypb.Empty{})
	r.waitFinished(t, traceMethod)
	if !proto.Equal(status.Convert(callErr).Proto(), want.Proto()) {
		t.Fatalf("logging changed upstream response: %v", callErr)
	}
	var requestID string
	select {
	case requestID = <-upstreamID:
	case <-ctx.Done():
		t.Fatal("no request ID observed upstream")
	}
	if requestID == "" {
		t.Fatal("upstream request was missing correlation ID")
	}
	for _, private := range []string{"private-error-message", traceDenied, "private-response-detail", "private-request-body", "private-metadata-value"} {
		if strings.Contains(logs.String(), private) {
			t.Errorf("RPC outcome log included request/response data: %q", private)
		}
	}
	var outcome map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("invalid JSON log: %v", err)
		}
		if event["msg"] == "grpc: RPC outcome" {
			if outcome != nil {
				t.Fatal("one RPC emitted multiple completion logs")
			}
			outcome = event
		}
	}
	if outcome == nil {
		t.Fatal("RPC completion was not logged at debug level")
	}
	for key, want := range map[string]any{
		"request_id": requestID, "backend": outcomeBackend, "protocol": "grpc", "method": traceMethod,
		"class": "by_height", "height": float64(145000000), "grpc_status": "Internal", "outcome": "neutral",
	} {
		if outcome[key] != want {
			t.Errorf("completion log %s=%v; want %v", key, outcome[key], want)
		}
	}
	if duration, ok := outcome["duration_ms"].(float64); !ok || duration < 0 {
		t.Errorf("completion log lacks a valid duration: %v", outcome["duration_ms"])
	}
}
