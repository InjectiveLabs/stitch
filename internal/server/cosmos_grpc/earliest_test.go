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
	"github.com/InjectiveLabs/stitch/internal/config"
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
	chainID     string
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
	if method == "/cosmos.base.tendermint.v1beta1.Service/GetNodeInfo" {
		chain := s.chainID
		if chain == "" {
			chain = "fixture-chain"
		}
		response := &emptypb.Empty{}
		response.ProtoReflect().SetUnknown(wireBytes(1, wireBytes(4, []byte(chain))))
		return stream.SendMsg(response)
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
	case "/cosmos.auth.v1beta1.Query/Params", "/cosmos.bank.v1beta1.Query/Params":
	case authMethod, codeMethod, "/injective.evm.v1.Query/EthCall", "/injective.evm.v1.Query/TraceBlock":
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
	sel     *selector.RangeSelector
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
	sel := selector.NewRangeSelector(reg, h, cm, 0)
	front, err := cosmosgrpc.New("127.0.0.1:0", cosmosgrpc.NewDirector(sel, cm, gp))
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
	return &historyRig{conn: conn, reg: reg, health: h, circuit: cm, sel: sel}
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

func (r *historyRig) archive(height int64) {
	r.sel.SetArchiveProfile(&config.ArchiveProfile{EVMStartHeight: height, CosmosChainID: "fixture-chain"})
}

func TestEarliestRequiresExplicitArchiveProfile(t *testing.T) {
	shard := &historyShard{name: "available", lower: 1, upper: 100}
	r := newHistoryRig(t, shard)
	for _, mode := range []string{"earliest", "archive-profile"} {
		_, _, _, err := invokeHistory(context.Background(), r.conn, paramsMethod, metadata.Pairs("stitch", mode), nil)
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("unconfigured %s: %v", mode, err)
		}
	}
	if len(shard.snapshot()) != 0 {
		t.Fatal("unconfigured profile probed a backend")
	}
}

func TestUnconfiguredHistoricalQueriesKeepTransparentPath(t *testing.T) {
	shard := &historyShard{name: "legacy", lower: 1, upper: 100, omitHeight: true, chainID: "unchecked-legacy-chain", payload: wireBytes(1, []byte("legacy-code"))}
	r := newHistoryRig(t, shard)
	for _, md := range []metadata.MD{metadata.Pairs(heightKey, "50"), {}} {
		body, headers, _, err := invokeHistory(context.Background(), r.conn, codeMethod, md, wireBytes(1, []byte("address")))
		if err != nil || !bytes.Equal(body, shard.payload) || len(headers.Get(resolvedKey)) != 0 {
			t.Fatalf("legacy response changed: %x %v %v", body, headers, err)
		}
	}
	for _, call := range shard.snapshot() {
		if call.method != codeMethod {
			t.Fatalf("legacy request performed archive validation: %s", call.method)
		}
	}
}

func TestEarliestUsesDeclaredStartWithoutScanning(t *testing.T) {
	for _, e := range []int64{47, 809, 32109} {
		t.Run(strconv.FormatInt(e, 10), func(t *testing.T) {
			shard := &historyShard{name: "history", lower: 1, upper: e + 100, stateFloor: e - 3, evmFloor: e - 3}
			r := newHistoryRig(t, shard)
			r.archive(e)
			_, headers, trailers, err := invokeHistory(context.Background(), r.conn, paramsMethod, earliestMD(), nil)
			if err != nil || firstValue(headers, resolvedKey) != strconv.FormatInt(e, 10) || firstValue(headers, heightKey) != strconv.FormatInt(e, 10) {
				t.Fatalf("profile height changed: %v %v", headers, err)
			}
			if firstValue(trailers, "fixture-trailer") != shard.name {
				t.Fatal("selected trailer lost")
			}
			calls := shard.snapshot()
			if len(calls) != 2 {
				t.Fatalf("expected identity + exact query, got %d calls", len(calls))
			}
			for _, call := range calls {
				if call.method == paramsMethod && call.height != e {
					t.Fatalf("scanned arbitrary height %d", call.height)
				}
				for _, key := range []string{"stitch", "x-stitch-earliest-capability", "x-stitch-backend", "x-stitch-cosmos-chain-id"} {
					if len(call.md.Get(key)) != 0 {
						t.Fatalf("leaked %s metadata", key)
					}
				}
			}
		})
	}
}

func TestEarliestDoesNotAdvanceWhenConfiguredStateIsMissing(t *testing.T) {
	const e int64 = 601
	shard := &historyShard{name: "newer-retained-state", lower: 1, upper: 1000, stateFloor: e + 1, evmFloor: e}
	r := newHistoryRig(t, shard)
	r.archive(e)
	_, headers, _, err := invokeHistory(context.Background(), r.conn, paramsMethod, earliestMD(), nil)
	if status.Code(err) != codes.Unavailable || len(headers.Get(resolvedKey)) != 0 {
		t.Fatalf("missing fixed height advanced: %v %v", headers, err)
	}
	for _, call := range shard.snapshot() {
		if call.method == paramsMethod && call.height != e {
			t.Fatalf("queried newer state %d", call.height)
		}
	}
}

func TestEarliestPreservesEmptyStateAndAccountAbsence(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(strconv.FormatBool(missing), func(t *testing.T) {
			shard := &historyShard{name: "empty", lower: 1, upper: 100, evmFloor: 50}
			if missing {
				shard.queryErr = status.Error(codes.NotFound, "account not found")
			}
			r := newHistoryRig(t, shard)
			r.archive(50)
			body, headers, _, err := invokeHistory(context.Background(), r.conn, authMethod, earliestMD(), wireBytes(1, []byte("address")))
			if missing && status.Code(err) != codes.NotFound {
				t.Fatalf("absence not preserved: %v", err)
			}
			if !missing && err != nil {
				t.Fatal(err)
			}
			if len(body) != 0 || firstValue(headers, heightKey) != "50" {
				t.Fatalf("empty state altered: %x %v", body, headers)
			}
			if missing {
				witnessed := false
				for _, call := range shard.snapshot() {
					if call.method == "/cosmos.auth.v1beta1.Query/Params" && call.height == 50 {
						witnessed = true
					}
				}
				if !witnessed {
					t.Fatal("account absence lacked exact state witness")
				}
			}
		})
	}
}

func TestHistoricalReplayRetriesActualRPCAtSameHeight(t *testing.T) {
	for _, kind := range []string{"transport", "missing-state", "wrong-height", "wrong-chain"} {
		t.Run(kind, func(t *testing.T) {
			failed := &historyShard{name: "first", lower: 1, upper: 1000, evmFloor: 400}
			good := &historyShard{name: "second", lower: 1, upper: 1000, evmFloor: 400, payload: wireBytes(1, []byte("correct"))}
			switch kind {
			case "transport":
				failed.queryErr = status.Error(codes.Unavailable, "temporarily down")
			case "missing-state":
				failed.queryFloor = 401
			case "wrong-height":
				failed.wrongHeight = 401
			case "wrong-chain":
				failed.chainID = "another-chain"
			}
			r := newHistoryRig(t, failed, good)
			r.archive(400)
			bs := r.reg.Snapshot()
			bs[0].Weight = 1000
			r.reg.Set(bs)
			body, headers, _, err := invokeHistory(context.Background(), r.conn, codeMethod, metadata.Pairs(heightKey, "400"), wireBytes(1, []byte("0xabc")))
			if err != nil || !bytes.Equal(body, good.payload) || firstValue(headers, heightKey) != "400" {
				t.Fatalf("same-height failover failed: %x %v %v", body, headers, err)
			}
			for _, s := range []*historyShard{failed, good} {
				for _, call := range s.snapshot() {
					if call.method == codeMethod && call.height != 400 {
						t.Fatalf("retry changed height: %d", call.height)
					}
				}
			}
		})
	}
}

func TestHistoricalReplayPreservesDomainErrors(t *testing.T) {
	failed := &historyShard{name: "revert", lower: 1, upper: 100, queryErr: status.Error(codes.Unknown, "execution reverted")}
	later := &historyShard{name: "later", lower: 1, upper: 100, payload: wireBytes(1, []byte("success"))}
	r := newHistoryRig(t, failed, later)
	r.archive(50)
	bs := r.reg.Snapshot()
	bs[0].Weight = 1000
	r.reg.Set(bs)
	_, _, _, err := invokeHistory(context.Background(), r.conn, "/injective.evm.v1.Query/EthCall", metadata.Pairs(heightKey, "50"), nil)
	if status.Code(err) != codes.Unknown || status.Convert(err).Message() != "execution reverted" {
		t.Fatalf("application error altered: %v", err)
	}
	if len(later.snapshot()) != 0 {
		t.Fatal("application error retried for a different answer")
	}
}

func TestHistoricalAbsenceRequiresValidSnapshot(t *testing.T) {
	shard := &historyShard{name: "unverified", lower: 1, upper: 100, queryErr: status.Error(codes.NotFound, "account not found"), omitHeight: true}
	r := newHistoryRig(t, shard)
	r.archive(50)
	_, _, _, err := invokeHistory(context.Background(), r.conn, authMethod, metadata.Pairs(heightKey, "50"), nil)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("missing witness falsely returned absence: %v", err)
	}
}

func TestHistoricalReplayHonorsHealthDrainAndCircuit(t *testing.T) {
	for _, gate := range []string{"drain", "grpc-health", "rpc-health", "circuit"} {
		t.Run(gate, func(t *testing.T) {
			shard := &historyShard{name: "excluded", lower: 1, upper: 100}
			r := newHistoryRig(t, shard)
			r.archive(50)
			switch gate {
			case "drain":
				r.reg.Drain(shard.name)
			case "grpc-health":
				r.health.Update(healthreg.Snapshot{Backend: shard.name, Protocol: types.ProtoGRPC, Healthy: false})
			case "rpc-health":
				r.health.Update(healthreg.Snapshot{Backend: shard.name, Protocol: types.ProtoRPC, Healthy: false})
			case "circuit":
				r.circuit.Record(shard.name, types.ProtoGRPC, false)
			}
			_, _, _, err := invokeHistory(context.Background(), r.conn, codeMethod, metadata.Pairs(heightKey, "50"), nil)
			if status.Code(err) != codes.Unavailable || len(shard.snapshot()) != 0 {
				t.Fatalf("excluded backend used: %v calls=%d", err, len(shard.snapshot()))
			}
		})
	}
}

func TestHistoricalCallerDeadlineCancelsReplay(t *testing.T) {
	shard := &historyShard{name: "slow", lower: 1, upper: 100, delay: time.Second}
	r := newHistoryRig(t, shard)
	r.archive(50)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, _, _, err := invokeHistory(ctx, r.conn, codeMethod, metadata.Pairs(heightKey, "50"), nil)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline not honored: %v", err)
	}
}

func TestEarliestRejectsCandidateSetBeyondBudget(t *testing.T) {
	var shards []*historyShard
	for i := 0; i < 257; i++ {
		shards = append(shards, &historyShard{name: fmt.Sprintf("shard-%d", i), lower: 1, upper: 100})
	}
	r := newHistoryRig(t, shards...)
	r.archive(50)
	_, _, _, err := invokeHistory(context.Background(), r.conn, paramsMethod, earliestMD(), nil)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("candidate budget not enforced: %v", err)
	}
}

func TestHistoricalLegacyAffinityIsStrict(t *testing.T) {
	first := &historyShard{name: "unavailable", lower: 1, upper: 100, queryErr: status.Error(codes.Unavailable, "down")}
	second := &historyShard{name: "available", lower: 1, upper: 100}
	r := newHistoryRig(t, first, second)
	r.archive(50)
	_, _, _, err := invokeHistory(context.Background(), r.conn, codeMethod, metadata.Pairs(heightKey, "50", "x-stitch-backend", first.name), nil)
	if status.Code(err) != codes.Unavailable || len(second.snapshot()) != 0 {
		t.Fatalf("legacy hard pin bypassed: %v", err)
	}
}
