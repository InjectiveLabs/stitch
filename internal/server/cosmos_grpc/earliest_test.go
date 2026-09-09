package cosmos_grpc_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	healthreg "github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/selector"
	cosmosgrpc "github.com/InjectiveLabs/stitch/internal/server/cosmos_grpc"
	"github.com/InjectiveLabs/stitch/internal/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/emptypb"
)

// These wire names and field numbers intentionally do not come from the
// production resolver. They are the public contract exercised over real TCP.
const (
	paramsMethod = "/injective.evm.v1.Query/Params"
	authMethod   = "/cosmos.auth.v1beta1.Query/Account"
	codeMethod   = "/injective.evm.v1.Query/Code"
	heightKey    = "x-cosmos-block-height"
	resolvedKey  = "x-stitch-earliest-height"
)

type historyCall struct {
	method string
	height int64
	md     metadata.MD
	body   []byte
}

type historyShard struct {
	name        string
	lower       int64
	upper       int64
	evmFloor    int64
	stateFloor  int64
	queryFloor  int64
	delay       time.Duration
	err         error
	queryErr    error
	payload     []byte
	omitHeight  bool
	wrongHeight int64
	rpcURL      string
	mu          sync.Mutex
	calls       []historyCall
	srv         *grpc.Server
	addr        string
	active      *atomic.Int64
	maxActive   *atomic.Int64
}

func (s *historyShard) start(t *testing.T) *backend.Backend {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.addr = lis.Addr().String()
	s.srv = grpc.NewServer(grpc.UnknownServiceHandler(s.handle))
	go func() { _ = s.srv.Serve(lis) }()
	t.Cleanup(s.srv.Stop)
	b := &backend.Backend{
		Name: s.name, Coverage: backend.Coverage{Kind: backend.CovBounded, Lower: s.lower, Upper: s.upper},
		Weight: 100, Endpoints: map[types.Protocol]string{types.ProtoGRPC: s.addr},
	}
	if s.rpcURL != "" {
		b.Endpoints[types.ProtoRPC] = s.rpcURL
	}
	return b
}

func (s *historyShard) handle(_ any, stream grpc.ServerStream) error {
	var req emptypb.Empty
	if err := stream.RecvMsg(&req); err != nil {
		return err
	}
	method, _ := grpc.MethodFromServerStream(stream)
	md, _ := metadata.FromIncomingContext(stream.Context())
	height, _ := strconv.ParseInt(firstValue(md, heightKey), 10, 64)
	if height == 0 {
		height = s.upper
	}
	s.mu.Lock()
	s.calls = append(s.calls, historyCall{method: method, height: height, md: md.Copy(), body: append([]byte(nil), req.ProtoReflect().GetUnknown()...)})
	s.mu.Unlock()
	if s.active != nil {
		n := s.active.Add(1)
		defer s.active.Add(-1)
		for previous := s.maxActive.Load(); n > previous; previous = s.maxActive.Load() {
			if s.maxActive.CompareAndSwap(previous, n) {
				break
			}
		}
	}
	if s.delay > 0 {
		timer := time.NewTimer(s.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		}
	}
	if s.err != nil {
		return s.err
	}
	if height < s.lower || height > s.upper || height < s.stateFloor {
		return status.Errorf(codes.Internal, "failed to load state at height %d; version mismatch on immutable IAVL tree; version does not exist. Version has either been pruned, or is for a future block height (latest height: %d)", height, s.upper)
	}
	responseHeight := height
	if s.wrongHeight != 0 {
		responseHeight = s.wrongHeight
	}
	header := metadata.Pairs("fixture-backend", s.name)
	if !s.omitHeight {
		header.Set(heightKey, strconv.FormatInt(responseHeight, 10))
	}
	if err := stream.SendHeader(header); err != nil {
		return err
	}
	stream.SetTrailer(metadata.Pairs("fixture-trailer", s.name))
	var payload []byte
	switch method {
	case paramsMethod:
		if height >= s.evmFloor {
			// QueryParamsResponse.params (1).evm_denom (1).
			payload = wireBytes(1, wireBytes(1, []byte("inj")))
		}
	case authMethod, codeMethod:
		if height < s.queryFloor {
			return status.Errorf(codes.Unknown, "failed to load state at height %d; version mismatch on immutable IAVL tree; version does not exist. Version has either been pruned, or is for a future block height (latest height: %d)", height, s.upper)
		}
		if s.queryErr != nil {
			return s.queryErr
		}
		payload = s.payload
	default:
		return status.Error(codes.Unimplemented, "fixture method unsupported")
	}
	resp := &emptypb.Empty{}
	resp.ProtoReflect().SetUnknown(payload)
	return stream.SendMsg(resp)
}

func (s *historyShard) snapshot() []historyCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]historyCall(nil), s.calls...)
}

type historyRig struct {
	conn    *grpc.ClientConn
	reg     *backend.Registry
	health  *healthreg.Registry
	circuit *circuit.Manager
}

func newHistoryRig(t *testing.T, shards ...*historyShard) *historyRig {
	t.Helper()
	var bs []*backend.Backend
	h := healthreg.NewRegistry()
	for _, shard := range shards {
		b := shard.start(t)
		bs = append(bs, b)
		h.Update(healthreg.Snapshot{Backend: b.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: shard.upper})
		h.Update(healthreg.Snapshot{Backend: b.Name, Protocol: types.ProtoGRPC, Healthy: true, LatestHeight: shard.upper})
	}
	reg := backend.NewRegistry(bs)
	cm := circuit.NewManager(circuit.Policy{ErrorThreshold: 0.1, MinRequests: 1, OpenDuration: time.Minute})
	gp := pool.NewGRPCPool(time.Minute)
	t.Cleanup(gp.CloseAll)
	front, err := cosmosgrpc.New("127.0.0.1:0", cosmosgrpc.NewDirector(selector.NewRangeSelector(reg, h, cm, 0), cm, gp))
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
	return &historyRig{conn: conn, reg: reg, health: h, circuit: cm}
}

func invokeHistory(ctx context.Context, conn *grpc.ClientConn, method string, md metadata.MD, payload []byte) ([]byte, metadata.MD, metadata.MD, error) {
	ctx = metadata.NewOutgoingContext(ctx, md)
	request := &emptypb.Empty{}
	request.ProtoReflect().SetUnknown(payload)
	var response emptypb.Empty
	var headers, trailers metadata.MD
	err := conn.Invoke(ctx, method, request, &response, grpc.Header(&headers), grpc.Trailer(&trailers))
	return response.ProtoReflect().GetUnknown(), headers, trailers, err
}

func earliestMD() metadata.MD {
	return metadata.Pairs("stitch", "earliest", heightKey, "1")
}

func firstValue(md metadata.MD, key string) string {
	values := md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func wireBytes(field protowire.Number, value []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, field, protowire.BytesType), value)
}

func TestEarliestDiscoversDataBeyondCosmosCoverage(t *testing.T) {
	old := &historyShard{name: "cosmos-before-evm", lower: 118_000_000, upper: 130_000_000, evmFloor: 127_123_456, stateFloor: 118_000_000}
	r := newHistoryRig(t, old)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, headers, trailers, err := invokeHistory(ctx, r.conn, paramsMethod, earliestMD(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{heightKey, resolvedKey} {
		if got := firstValue(headers, key); got != "127123456" {
			t.Errorf("%s=%q; want verified EVM floor 127123456", key, got)
		}
	}
	if got := firstValue(trailers, "fixture-trailer"); got != old.name {
		t.Errorf("winner trailer lost: %v", trailers)
	}
	calls := old.snapshot()
	if len(calls) > 64 {
		t.Errorf("discovery used %d RPCs for a 12M-block range", len(calls))
	}
	for _, call := range calls {
		if call.height < old.lower || call.height > old.upper {
			t.Errorf("out-of-coverage request at %d", call.height)
		}
		if len(call.md.Get("stitch")) != 0 || len(call.md.Get("x-stitch-earliest-capability")) != 0 {
			t.Errorf("routing metadata leaked to normal node: %v", call.md)
		}
	}
}

func TestEarliestSelectsActualHeightDespiteConfiguredOrderAndSpeed(t *testing.T) {
	newer := &historyShard{name: "first-configured-newer-data", lower: 118_000_000, upper: 150_000_000, evmFloor: 140_000_000, stateFloor: 118_000_000, payload: wireBytes(1, []byte("newer"))}
	older := &historyShard{name: "later-configured-oldest-data", lower: 120_000_000, upper: 150_000_000, evmFloor: 127_000_007, stateFloor: 120_000_000, delay: time.Millisecond, payload: wireBytes(1, []byte("older"))}
	r := newHistoryRig(t, newer, older)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	body, headers, trailers, err := invokeHistory(ctx, r.conn, codeMethod, earliestMD(), wireBytes(1, []byte("0x0123")))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, older.payload) || firstValue(headers, resolvedKey) != "127000007" {
		t.Fatalf("selected newer/faster/configuration-first response: body=%x headers=%v", body, headers)
	}
	if firstValue(headers, "fixture-backend") != older.name || firstValue(trailers, "fixture-trailer") != older.name {
		t.Errorf("winner metadata lost: headers=%v trailers=%v", headers, trailers)
	}
	for _, shard := range []*historyShard{newer, older} {
		for _, call := range shard.snapshot() {
			if call.method == codeMethod && (!bytes.Equal(call.body, wireBytes(1, []byte("0x0123"))) || call.height < shard.evmFloor) {
				t.Errorf("%s application request lost body or concrete height: %+v", shard.name, call)
			}
		}
	}
}

func TestEarliestPreservesEmptyStateAndAccountAbsence(t *testing.T) {
	for _, method := range []string{codeMethod, authMethod} {
		t.Run(method, func(t *testing.T) {
			older := &historyShard{name: "old-empty", lower: 127_000_000, upper: 127_000_010, evmFloor: 127_000_000}
			if method == authMethod {
				older.queryErr = status.Error(codes.NotFound, "account inj1missing not found")
			}
			newer := &historyShard{name: "new-present", lower: 128_000_000, upper: 128_000_010, evmFloor: 128_000_000, payload: wireBytes(1, []byte("present"))}
			r := newHistoryRig(t, newer, older)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			body, headers, _, err := invokeHistory(ctx, r.conn, method, earliestMD(), wireBytes(1, []byte("account")))
			if method == authMethod && status.Code(err) != codes.NotFound {
				t.Fatalf("valid old absence replaced: err=%v body=%x", err, body)
			}
			if method == codeMethod && (err != nil || len(body) != 0) {
				t.Fatalf("valid empty code replaced: err=%v body=%x", err, body)
			}
			if firstValue(headers, resolvedKey) != "127000000" {
				t.Errorf("absence/empty result height lost: %v", headers)
			}
			if state := r.circuit.State(older.name, types.ProtoGRPC); state != circuit.StateClosed {
				t.Errorf("domain absence opened backend circuit: %v", state)
			}
		})
	}
}

func TestEarliestRejectsInvalidMarkersAndUnsupportedMethods(t *testing.T) {
	tests := []struct {
		name   string
		method string
		md     metadata.MD
	}{
		{"unknown mode", paramsMethod, metadata.Pairs("stitch", "latest")},
		{"duplicate mode", paramsMethod, metadata.Pairs("stitch", "earliest", "stitch", "earliest")},
		{"unknown capability", paramsMethod, metadata.Pairs("stitch", "earliest", "x-stitch-earliest-capability", "unknown")},
		{"write", "/cosmos.tx.v1beta1.Service/BroadcastTx", earliestMD()},
		{"unknown unary", "/example.Query/Read", earliestMD()},
		{"streaming", "/grpc.health.v1.Health/Watch", earliestMD()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shard := &historyShard{name: "must-not-call", lower: 1, upper: 10, evmFloor: 1}
			r := newHistoryRig(t, shard)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _, _, err := invokeHistory(ctx, r.conn, tc.method, tc.md, nil)
			if err == nil {
				t.Fatal("invalid discovery request succeeded")
			}
			if calls := shard.snapshot(); len(calls) != 0 {
				t.Errorf("invalid request reached upstream: %+v", calls)
			}
		})
	}
}

func TestEarliestDoesNotRewriteOrdinaryNumericQueries(t *testing.T) {
	shard := &historyShard{name: "explicit", lower: 118_000_000, upper: 140_000_000, evmFloor: 127_000_000, payload: wireBytes(1, []byte("exact"))}
	r := newHistoryRig(t, shard)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	body, headers, _, err := invokeHistory(ctx, r.conn, codeMethod, metadata.Pairs(heightKey, "128000123"), wireBytes(1, []byte("address")))
	if err != nil || !bytes.Equal(body, shard.payload) {
		t.Fatalf("numeric query changed: body=%x err=%v", body, err)
	}
	if firstValue(headers, resolvedKey) != "" {
		t.Errorf("numeric query acquired discovery metadata: %v", headers)
	}
	if calls := shard.snapshot(); len(calls) != 1 || calls[0].height != 128_000_123 || calls[0].method != codeMethod {
		t.Errorf("numeric query was discovered or rewritten: %+v", calls)
	}
}

func TestEarliestDiscoversRequestedStateAvailability(t *testing.T) {
	// EVM parameters exist before this node can answer the actual query.
	// A Params-only search must not call the later shard the earliest result.
	older := &historyShard{name: "partial-state", lower: 118_000_000, upper: 150_000_000, evmFloor: 127_000_000, queryFloor: 128_123_456, payload: wireBytes(1, []byte("oldest-code"))}
	newer := &historyShard{name: "newer", lower: 140_000_000, upper: 150_000_000, evmFloor: 140_000_000, payload: wireBytes(1, []byte("newer-code"))}
	r := newHistoryRig(t, older, newer)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	body, headers, _, err := invokeHistory(ctx, r.conn, codeMethod, earliestMD(), wireBytes(1, []byte("address")))
	if err != nil || !bytes.Equal(body, older.payload) || firstValue(headers, resolvedKey) != "128123456" {
		t.Fatalf("actual state floor not discovered: body=%x headers=%v err=%v", body, headers, err)
	}
	if r.circuit.State(older.name, types.ProtoGRPC) != circuit.StateClosed {
		t.Fatal("expected missing historical versions poisoned the backend circuit")
	}
}

func TestEarliestSkipsShardsWithNoEVMData(t *testing.T) {
	preEVM := &historyShard{name: "pre-evm", lower: 118_000_000, upper: 126_999_999, evmFloor: 127_000_000}
	valid := &historyShard{name: "evm", lower: 127_000_000, upper: 130_000_000, evmFloor: 127_000_000}
	r := newHistoryRig(t, preEVM, valid)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, headers, _, err := invokeHistory(ctx, r.conn, paramsMethod, earliestMD(), nil)
	if err != nil || firstValue(headers, resolvedKey) != "127000000" {
		t.Fatalf("pre-EVM coverage prevented valid discovery: headers=%v err=%v", headers, err)
	}
	if state := r.circuit.State(preEVM.name, types.ProtoGRPC); state != circuit.StateClosed {
		t.Errorf("pre-EVM history opened live backend's circuit: %v", state)
	}
}

func TestEarliestValidatesUpstreamHeightEvidence(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(fmt.Sprintf("mismatch=%v", mismatch), func(t *testing.T) {
			shard := &historyShard{name: "unproven-height", lower: 127_000_000, upper: 127_000_010, evmFloor: 127_000_000, omitHeight: !mismatch}
			if mismatch {
				shard.wrongHeight = 127_000_005
			}
			r := newHistoryRig(t, shard)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _, _, err := invokeHistory(ctx, r.conn, paramsMethod, earliestMD(), nil)
			if err == nil {
				t.Fatal("returned unverified/mismatched response height as earliest")
			}
		})
	}
}

func TestEarliestHonorsDrainHealthCircuitAndPrunedExclusion(t *testing.T) {
	for _, exclusion := range []string{"drain", "grpc-health", "rpc-witness", "circuit", "pruned"} {
		t.Run(exclusion, func(t *testing.T) {
			older := &historyShard{name: "excluded", lower: 127_000_000, upper: 150_000_000, evmFloor: 127_000_000}
			newer := &historyShard{name: "eligible", lower: 140_000_000, upper: 150_000_000, evmFloor: 140_000_000}
			r := newHistoryRig(t, older, newer)
			switch exclusion {
			case "drain":
				r.reg.Drain(older.name)
			case "grpc-health":
				r.health.Update(healthreg.Snapshot{Backend: older.name, Protocol: types.ProtoGRPC, Healthy: false})
			case "rpc-witness":
				// Real bounded backends only have RPC witness snapshots. Remove
				// the fixture's protocol-specific snapshot before publishing it.
				r.health.Prune(map[string]struct{}{newer.name: {}})
				r.health.Update(healthreg.Snapshot{Backend: older.name, Protocol: types.ProtoRPC, Healthy: false})
			case "circuit":
				r.circuit.Record(older.name, types.ProtoGRPC, false)
			case "pruned":
				current := r.reg.Snapshot()
				copyBackend := *current[0]
				copyBackend.Coverage = backend.Coverage{Kind: backend.CovPruned, Keep: 30_000_001}
				r.reg.Set([]*backend.Backend{&copyBackend, current[1]})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, headers, _, err := invokeHistory(ctx, r.conn, paramsMethod, earliestMD(), nil)
			if exclusion == "drain" || exclusion == "pruned" {
				if err != nil || firstValue(headers, resolvedKey) != "140000000" {
					t.Fatalf("eligible discovery failed: headers=%v err=%v", headers, err)
				}
			} else if status.Code(err) != codes.Unavailable {
				t.Fatalf("health/circuit exclusion falsely proved older history absent: headers=%v err=%v", headers, err)
			}
			if calls := older.snapshot(); len(calls) != 0 {
				t.Fatalf("%s backend received %d requests", exclusion, len(calls))
			}
		})
	}
}

func TestEarliestRetriesActualRPCFailureOnReplica(t *testing.T) {
	failed := &historyShard{name: "rpc-fails-after-dial", lower: 127_000_000, upper: 130_000_000, evmFloor: 127_000_000, queryErr: status.Error(codes.Unavailable, "temporary application transport failure")}
	replica := &historyShard{name: "same-range-replica", lower: 127_000_000, upper: 130_000_000, evmFloor: 127_000_000, payload: wireBytes(1, []byte("recovered"))}
	r := newHistoryRig(t, failed, replica)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	body, headers, _, err := invokeHistory(ctx, r.conn, codeMethod, earliestMD(), wireBytes(1, []byte("address")))
	if err != nil || !bytes.Equal(body, replica.payload) || firstValue(headers, resolvedKey) != "127000000" {
		t.Fatalf("successful dial prevented RPC failover: body=%x headers=%v err=%v", body, headers, err)
	}
	if r.circuit.State(failed.name, types.ProtoGRPC) != circuit.StateOpen {
		t.Fatal("actual RPC failure not recorded against circuit")
	}
}

func TestEarliestDoesNotClaimCompleteAfterOlderFailure(t *testing.T) {
	older := &historyShard{name: "unknown-older", lower: 118_000_000, upper: 130_000_000, evmFloor: 127_000_000, err: status.Error(codes.Unavailable, "unreachable")}
	newer := &historyShard{name: "newer-success", lower: 140_000_000, upper: 150_000_000, evmFloor: 140_000_000}
	r := newHistoryRig(t, older, newer)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, _, err := invokeHistory(ctx, r.conn, paramsMethod, earliestMD(), nil)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unresolved older shard reported success/proven absence: %v", err)
	}
}

func TestEarliestCallerDeadlineCancelsFanout(t *testing.T) {
	var active, maxActive atomic.Int64
	shard := &historyShard{name: "slow", lower: 118_000_000, upper: 150_000_000, evmFloor: 127_000_000, delay: time.Second, active: &active, maxActive: &maxActive}
	r := newHistoryRig(t, shard)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, _, err := invokeHistory(ctx, r.conn, paramsMethod, earliestMD(), nil)
	if status.Code(err) != codes.DeadlineExceeded || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("discovery exceeded caller budget: elapsed=%v err=%v", time.Since(start), err)
	}
	for until := time.Now().Add(time.Second); active.Load() != 0 && time.Now().Before(until); {
		time.Sleep(time.Millisecond)
	}
	if active.Load() != 0 {
		t.Fatal("upstream RPC survived caller cancellation")
	}
}

func TestEarliestBoundsFanoutConcurrency(t *testing.T) {
	var active, maxActive atomic.Int64
	var shards []*historyShard
	for i := 0; i < 12; i++ {
		shards = append(shards, &historyShard{name: fmt.Sprintf("bounded-%02d", i), lower: 127_000_000, upper: 127_000_000, evmFloor: 127_000_000, delay: 5 * time.Millisecond, active: &active, maxActive: &maxActive})
	}
	r := newHistoryRig(t, shards...)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, _, err := invokeHistory(ctx, r.conn, paramsMethod, earliestMD(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if peak := maxActive.Load(); peak < 2 || peak > 4 {
		t.Errorf("fanout concurrency=%d; want bounded parallelism 2..4", peak)
	}
}

func TestEarliestResolvedBackendAffinityPreservesDependentState(t *testing.T) {
	preferred := &historyShard{name: "preferred-but-missing", lower: 118_000_000, upper: 150_000_000, evmFloor: 140_000_000, queryFloor: 140_000_000, payload: wireBytes(1, []byte("newer"))}
	oldest := &historyShard{name: "discovered", lower: 118_000_000, upper: 150_000_000, evmFloor: 127_000_007, payload: wireBytes(1, []byte("oldest"))}
	r := newHistoryRig(t, preferred, oldest)
	bs := r.reg.Snapshot()
	weighted := *bs[0]
	weighted.Weight = 1000
	r.reg.Set([]*backend.Backend{&weighted, bs[1]})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, headers, _, err := invokeHistory(ctx, r.conn, paramsMethod, earliestMD(), nil)
	if err != nil || firstValue(headers, "x-stitch-backend") != oldest.name {
		t.Fatalf("resolved backend identity missing: headers=%v err=%v", headers, err)
	}
	pinned := metadata.Pairs(heightKey, firstValue(headers, resolvedKey), "x-stitch-backend", firstValue(headers, "x-stitch-backend"))
	body, _, _, err := invokeHistory(ctx, r.conn, codeMethod, pinned, wireBytes(1, []byte("account")))
	if err != nil || !bytes.Equal(body, oldest.payload) {
		t.Fatalf("dependent read changed backend/snapshot: body=%x err=%v", body, err)
	}
	for _, call := range oldest.snapshot() {
		if call.method == codeMethod && len(call.md.Get("x-stitch-backend")) != 0 {
			t.Errorf("affinity metadata leaked to ordinary upstream: %v", call.md)
		}
	}
	for _, call := range preferred.snapshot() {
		if call.method == codeMethod {
			t.Fatal("dependent read used a high-weight overlapping but unverified backend")
		}
	}
}

func TestEarliestRejectsCandidateSetBeyondBudget(t *testing.T) {
	shard := &historyShard{name: "shared-fixture", lower: 127_000_000, upper: 127_000_001, evmFloor: 127_000_000}
	r := newHistoryRig(t, shard)
	var backends []*backend.Backend
	base := *r.reg.Snapshot()[0]
	for i := 0; i < 257; i++ {
		b := base
		b.Name = fmt.Sprintf("candidate-%d", i)
		backends = append(backends, &b)
	}
	r.reg.Set(backends)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, _, err := invokeHistory(ctx, r.conn, paramsMethod, earliestMD(), nil)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized candidate set accepted: %v", err)
	}
	if len(shard.snapshot()) != 0 {
		t.Fatal("candidate budget enforced after dispatching requests")
	}
}

func TestEarliestBoundsConcurrentDiscoveryAcrossRequests(t *testing.T) {
	var active, maxActive atomic.Int64
	var shards []*historyShard
	for i := 0; i < 8; i++ {
		shards = append(shards, &historyShard{name: fmt.Sprintf("shared-%02d", i), lower: 127_000_000, upper: 127_000_000, evmFloor: 127_000_000, delay: 5 * time.Millisecond, active: &active, maxActive: &maxActive})
	}
	r := newHistoryRig(t, shards...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var calls sync.WaitGroup
	for i := 0; i < 16; i++ {
		calls.Add(1)
		go func() {
			defer calls.Done()
			if _, _, _, err := invokeHistory(ctx, r.conn, paramsMethod, earliestMD(), nil); err != nil {
				t.Errorf("concurrent discovery: %v", err)
			}
		}()
	}
	calls.Wait()
	if peak := maxActive.Load(); peak < 2 || peak > 32 {
		t.Errorf("global discovery concurrency=%d; expected 2..32", peak)
	}
}
