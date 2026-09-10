package cosmos_grpc_test

// Independent wire fixtures exercise the public logical-archive contract. They
// are routing/error tests, not evidence of real Injective store/trace parity.
import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"sync"
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

const archiveContractBlockMethod = "/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight"
const archiveContractIdentityMethod = "/cosmos.base.tendermint.v1beta1.Service/GetNodeInfo"

type archiveContractCall struct {
	method string
	height int64
	md     metadata.MD
}

type archiveContractPeer struct {
	name, chain              string
	lower, upper, stateFloor int64
	failed                   bool
	wrongHeight              bool
	absent                   bool
	empty                    bool
	mu                       sync.Mutex
	calls                    []archiveContractCall
}

func (p *archiveContractPeer) serve(_ any, stream grpc.ServerStream) error {
	var request emptypb.Empty
	if err := stream.RecvMsg(&request); err != nil {
		return err
	}
	method, _ := grpc.MethodFromServerStream(stream)
	md, _ := metadata.FromIncomingContext(stream.Context())
	height, _ := strconv.ParseInt(firstValue(md, heightKey), 10, 64)
	if method == archiveContractBlockMethod {
		raw := request.ProtoReflect().GetUnknown()
		for len(raw) > 0 {
			field, kind, n := protowire.ConsumeTag(raw)
			if n < 0 {
				return status.Error(codes.InvalidArgument, "invalid block request")
			}
			raw = raw[n:]
			if field == 1 && kind == protowire.VarintType {
				value, n := protowire.ConsumeVarint(raw)
				if n < 0 {
					return status.Error(codes.InvalidArgument, "invalid block height")
				}
				if value > math.MaxInt64 {
					return status.Error(codes.InvalidArgument, "block height exceeds int64")
				}
				height = int64(value)
				raw = raw[n:]
			} else {
				n = protowire.ConsumeFieldValue(field, kind, raw)
				if n < 0 {
					return status.Error(codes.InvalidArgument, "invalid block field")
				}
				raw = raw[n:]
			}
		}
	}
	if height == 0 {
		height = p.upper
	}
	p.mu.Lock()
	p.calls = append(p.calls, archiveContractCall{method: method, height: height, md: md.Copy()})
	p.mu.Unlock()
	if p.failed {
		return status.Error(codes.Unavailable, "independent fixture unavailable")
	}
	if height < p.lower || height > p.upper || (method != archiveContractBlockMethod && method != archiveContractIdentityMethod && height < p.stateFloor) {
		return status.Errorf(codes.Internal, "failed to load state at height %d; version mismatch on immutable IAVL tree; version does not exist. Version has either been pruned, or is for a future block height (latest height: %d)", height, p.upper)
	}
	replyHeight := height
	if p.wrongHeight {
		replyHeight++
	}
	if err := stream.SendHeader(metadata.Pairs(heightKey, strconv.FormatInt(replyHeight, 10))); err != nil {
		return err
	}
	var raw []byte
	switch method {
	case paramsMethod:
		raw = wireBytes(1, wireBytes(1, []byte("inj")))
	case "/cosmos.auth.v1beta1.Query/Params":
		// A successful exact-height auth-store witness is independent of
		// whether the requested account exists in that snapshot.
		raw = wireBytes(1, nil)
	case archiveContractIdentityMethod:
		// SDK GetNodeInfoResponse.default_node_info(1).network(4).
		raw = wireBytes(1, wireBytes(4, []byte(p.chain)))
	case archiveContractBlockMethod:
		// Public SDK wire schema: response.sdk_block(3).header(1),
		// header.chain_id(2), header.height(3). No production codec used.
		header := wireBytes(2, []byte(p.chain))
		if replyHeight <= 0 {
			return status.Error(codes.InvalidArgument, "block height must be positive")
		}
		header = protowire.AppendTag(header, 3, protowire.VarintType)
		header = protowire.AppendVarint(header, uint64(replyHeight))
		raw = wireBytes(3, wireBytes(1, header))
	case codeMethod:
		if !p.empty {
			raw = wireBytes(1, []byte(fmt.Sprintf("state-at-%d", height)))
		}
	case authMethod:
		if p.absent {
			return status.Error(codes.NotFound, "account not found")
		}
		raw = wireBytes(1, []byte(fmt.Sprintf("account-at-%d", height)))
	default:
		return status.Error(codes.Unimplemented, "unexpected independent fixture method")
	}
	response := &emptypb.Empty{}
	response.ProtoReflect().SetUnknown(raw)
	return stream.SendMsg(response)
}

func (p *archiveContractPeer) snapshot() []archiveContractCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]archiveContractCall(nil), p.calls...)
}

func newArchiveContractRig(t *testing.T, start int64, chain string, peers ...*archiveContractPeer) *grpc.ClientConn {
	t.Helper()
	health := healthreg.NewRegistry()
	var backends []*backend.Backend
	for _, peer := range peers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		server := grpc.NewServer(grpc.UnknownServiceHandler(peer.serve))
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(server.Stop)
		backends = append(backends, &backend.Backend{Name: peer.name, Weight: 100, Coverage: backend.Coverage{Kind: backend.CovBounded, Lower: peer.lower, Upper: peer.upper}, Endpoints: map[types.Protocol]string{types.ProtoGRPC: listener.Addr().String()}})
		for _, protocol := range []types.Protocol{types.ProtoRPC, types.ProtoGRPC} {
			health.Update(healthreg.Snapshot{Backend: peer.name, Protocol: protocol, Healthy: true, LatestHeight: peer.upper})
		}
	}
	manager := circuit.NewManager(circuit.Policy{MinRequests: 1000, ErrorThreshold: 0.9, OpenDuration: time.Second})
	selection := selector.NewRangeSelector(backend.NewRegistry(backends), health, manager, 0)
	selection.SetArchiveProfile(&config.ArchiveProfile{EVMStartHeight: start, CosmosChainID: chain})
	connections := pool.NewGRPCPool(time.Minute)
	t.Cleanup(connections.CloseAll)
	front, err := cosmosgrpc.New("127.0.0.1:0", cosmosgrpc.NewDirector(selection, manager, connections))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = front.Start(context.Background()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = front.Shutdown(ctx)
	})
	connection, err := grpc.NewClient(front.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func TestArchiveContractGeneratedSeamsPreserveExactHeight(t *testing.T) {
	rng := rand.New(rand.NewSource(971309)) //nolint:gosec // Reproducible test topologies do not require cryptographic randomness.
	for run := 0; run < 9; run++ {
		start := int64(3 + rng.Intn(900000))
		first := start + int64(3+rng.Intn(13))
		second := first + int64(3+rng.Intn(13))
		head := second + int64(3+rng.Intn(13))
		t.Run(fmt.Sprintf("E_%d_seams_%d_%d", start, first, second), func(t *testing.T) {
			chain := fmt.Sprintf("independent-%d", run)
			peers := []*archiveContractPeer{
				{name: "old", chain: chain, lower: start, upper: first, stateFloor: start},
				{name: "middle", chain: chain, lower: first, upper: second, stateFloor: first},
				{name: "new", chain: chain, lower: second, upper: head, stateFloor: second},
			}
			rng.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
			connection := newArchiveContractRig(t, start, chain, peers...)
			for _, height := range []int64{start, first - 1, first, first + 1, second - 1, second, second + 1, head} {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				body, headers, _, err := invokeHistory(ctx, connection, codeMethod, metadata.Pairs(heightKey, strconv.FormatInt(height, 10)), nil)
				cancel()
				want := wireBytes(1, []byte(fmt.Sprintf("state-at-%d", height)))
				if err != nil || !bytes.Equal(body, want) || firstValue(headers, heightKey) != strconv.FormatInt(height, 10) {
					t.Fatalf("numeric height %d changed or missing: body=%x headers=%v err=%v", height, body, headers, err)
				}
			}
			for _, peer := range peers {
				for _, call := range peer.snapshot() {
					if call.height < peer.lower || call.height > peer.upper {
						t.Errorf("request outside retained range: %s %+v", peer.name, call)
					}
					for _, key := range []string{"stitch", "x-stitch-backend", "x-stitch-earliest-capability"} {
						if len(call.md.Get(key)) != 0 {
							t.Errorf("private metadata leaked: %s %+v", key, call)
						}
					}
				}
			}
		})
	}
}

func TestArchiveContractEarliestNeverAdvancesForCapabilityOrOutage(t *testing.T) {
	for _, capability := range []string{"state", "execution", "block", "range", "trace", "proof", "storage"} {
		for _, failure := range []string{"offline", "missing-state"} {
			t.Run(capability+"/"+failure, func(t *testing.T) {
				const start int64 = 413
				old := &archiveContractPeer{name: "old", chain: "contract-7", lower: start, upper: start + 8, stateFloor: start}
				if failure == "offline" {
					old.failed = true
				} else {
					old.stateFloor = start + 1
				}
				newer := &archiveContractPeer{name: "new", chain: "contract-7", lower: start + 8, upper: start + 20, stateFloor: start + 8}
				connection := newArchiveContractRig(t, start, "contract-7", newer, old)
				md := earliestMD()
				md.Set("x-stitch-earliest-capability", capability)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				body, _, _, err := invokeHistory(ctx, connection, paramsMethod, md, nil)
				if err == nil {
					t.Fatalf("lost E became successful later Params: %x", body)
				}
				for _, peer := range []*archiveContractPeer{old, newer} {
					for _, call := range peer.snapshot() {
						if call.method != archiveContractIdentityMethod && call.height != start {
							t.Errorf("earliest advanced or searched: %+v", call)
						}
					}
				}
			})
		}
	}
}

func TestArchiveContractSameHeightReplicaPreservesEmptyAndAbsence(t *testing.T) {
	for _, method := range []string{codeMethod, authMethod} {
		t.Run(method, func(t *testing.T) {
			const start int64 = 7019
			dead := &archiveContractPeer{name: "a-dead", chain: "contract-9", lower: start, upper: start + 10, stateFloor: start, failed: true}
			good := &archiveContractPeer{name: "b-good", chain: "contract-9", lower: start, upper: start + 10, stateFloor: start, empty: true, absent: true}
			connection := newArchiveContractRig(t, start, "contract-9", dead, good)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			body, headers, _, err := invokeHistory(ctx, connection, method, metadata.Pairs(heightKey, strconv.FormatInt(start, 10)), nil)
			if method == codeMethod && (err != nil || len(body) != 0) {
				t.Fatalf("empty state became failure/present value: %x %v", body, err)
			}
			if method == authMethod && status.Code(err) != codes.NotFound {
				t.Fatalf("absence became other result: %x %v", body, err)
			}
			if firstValue(headers, heightKey) != strconv.FormatInt(start, 10) {
				t.Errorf("result lost exact height: %v", headers)
			}
			for _, peer := range []*archiveContractPeer{dead, good} {
				for _, call := range peer.snapshot() {
					if call.method != archiveContractIdentityMethod && call.height != start {
						t.Errorf("retry changed height: %+v", call)
					}
				}
			}
		})
	}
}

func TestArchiveContractRejectsMismatchedChainOrHeight(t *testing.T) {
	for _, mismatch := range []string{"chain", "height"} {
		t.Run(mismatch, func(t *testing.T) {
			peer := &archiveContractPeer{name: "wrong", chain: "contract-11", lower: 1901, upper: 1920, stateFloor: 1901}
			if mismatch == "chain" {
				peer.chain = "another-chain"
			} else {
				peer.wrongHeight = true
			}
			connection := newArchiveContractRig(t, 1901, "contract-11", peer)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			body, _, _, err := invokeHistory(ctx, connection, codeMethod, metadata.Pairs(heightKey, "1907"), nil)
			if err == nil {
				t.Fatalf("contradictory %s accepted: %x", mismatch, body)
			}
		})
	}
}

func TestArchiveContractProfileHandshakeDoesNotDependOnLiveShards(t *testing.T) {
	peer := &archiveContractPeer{name: "offline", chain: "contract-15", lower: 301, upper: 340, stateFloor: 301, failed: true}
	connection := newArchiveContractRig(t, 301, "contract-15", peer)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, headers, _, err := invokeHistory(ctx, connection, paramsMethod, metadata.Pairs("stitch", "archive-profile"), nil)
	if err != nil || firstValue(headers, resolvedKey) != "301" || firstValue(headers, heightKey) != "301" || firstValue(headers, "x-stitch-cosmos-chain-id") != "contract-15" {
		t.Fatalf("logical profile handshake changed with liveness: %v %v", headers, err)
	}
	if calls := peer.snapshot(); len(calls) != 0 {
		t.Fatalf("profile handshake contacted historical nodes: %+v", calls)
	}
}
