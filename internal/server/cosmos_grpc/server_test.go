package cosmos_grpc

import (
	"bytes"
	"context"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	healthreg "github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// Kept independent from Manifest on purpose: these are schema assertions, so
// a deleted method or accidental field-number edit must fail tests rather than
// changing the test input along with production.
var expectedBodyHeightMethods = map[string]MethodSpec{
	"/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight": {
		HeightField: 1,
		HeightName:  "height",
	},
	"/cosmos.base.tendermint.v1beta1.Service/GetValidatorSetByHeight": {
		HeightField: 1,
		HeightName:  "height",
	},
	"/cosmos.base.tendermint.v1beta1.Service/ABCIQuery": {
		HeightField: 3,
		HeightName:  "height",
	},
	"/cosmos.staking.v1beta1.Query/HistoricalInfo": {
		HeightField: 1,
		HeightName:  "height",
	},
	"/cosmos.tx.v1beta1.Service/GetBlockWithTxs": {
		HeightField: 1,
		HeightName:  "height",
	},
	"/ibc.applications.fee.v1.Query/IncentivizedPackets": {
		HeightField: 2,
		HeightName:  "query_height",
	},
	"/ibc.applications.fee.v1.Query/IncentivizedPacket": {
		HeightField: 2,
		HeightName:  "query_height",
	},
	"/ibc.applications.fee.v1.Query/IncentivizedPacketsForChannel": {
		HeightField: 4,
		HeightName:  "query_height",
	},
	"/ibc.applications.fee.v1.Query/FeeEnabledChannels": {
		HeightField: 2,
		HeightName:  "query_height",
	},
}

// mockBackend wraps grpc.health.v1.Health on a real listener with hit
// counting and a kill switch.
type mockBackend struct {
	name         string
	hits         atomic.Int64
	dead         atomic.Bool
	body         atomic.Value
	heightHeader atomic.Value
	srv          *grpc.Server
	lis          net.Listener
}

func newMockBackend(t *testing.T, name string) *mockBackend {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mb := &mockBackend{name: name, lis: lis}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(mb.handleUnknown))
	mb.srv = srv
	hsrv := &countingHealthServer{mb: mb, inner: health.NewServer()}
	healthpb.RegisterHealthServer(srv, hsrv)
	go func() { _ = srv.Serve(lis) }()
	return mb
}

func (m *mockBackend) Addr() string { return m.lis.Addr().String() }
func (m *mockBackend) Stop()        { m.srv.Stop() }

func (m *mockBackend) handleUnknown(_ any, stream grpc.ServerStream) error {
	m.hits.Add(1)
	if m.dead.Load() {
		return grpcUnavailable("dead")
	}
	var req emptypb.Empty
	if err := stream.RecvMsg(&req); err != nil {
		return err
	}
	payload := append([]byte(nil), req.ProtoReflect().GetUnknown()...)
	m.body.Store(payload)
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		if values := md.Get(HeightHeader); len(values) > 0 {
			m.heightHeader.Store(values[0])
		}
	}
	return stream.SendMsg(&emptypb.Empty{})
}

// countingHealthServer increments a hit counter and returns either a
// SERVING response with the backend name in the service field, or fails
// when the kill switch is set.
type countingHealthServer struct {
	healthpb.UnimplementedHealthServer
	mb    *mockBackend
	inner *health.Server
}

func (s *countingHealthServer) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	s.mb.hits.Add(1)
	if s.mb.dead.Load() {
		return nil, grpcUnavailable("dead")
	}
	// Encode backend name into the response status by appending. Clients
	// inspect req.Service for the routing assertion via mocked echo.
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

// rig is the same shape as Phase 1's integration rig but for gRPC.
type rig struct {
	front     *Server
	frontConn *grpc.ClientConn
	archive   *mockBackend
	shard     *mockBackend
	pool      *pool.GRPCPool
}

func (r *rig) close() {
	_ = r.frontConn.Close()
	_ = r.front.Shutdown(context.Background())
	r.archive.Stop()
	r.shard.Stop()
	r.pool.CloseAll()
}

func setupGRPC(t *testing.T) *rig {
	t.Helper()
	a := newMockBackend(t, "archive")
	s := newMockBackend(t, "shard1")

	bs := []*backend.Backend{
		{
			Name:      "archive",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoGRPC: a.Addr()},
		},
		{
			Name:      "shard1",
			Coverage:  backend.Coverage{Kind: backend.CovBounded, Lower: 1, Upper: 50000},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoGRPC: s.Addr()},
		},
	}
	reg := backend.NewRegistry(bs)
	h := healthreg.NewRegistry()
	for _, bb := range bs {
		h.Update(healthreg.Snapshot{
			Backend: bb.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000,
		})
		h.Update(healthreg.Snapshot{
			Backend: bb.Name, Protocol: types.ProtoGRPC, Healthy: true,
		})
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   100 * time.Millisecond,
	})
	sel := selector.NewRangeSelector(reg, h, cm, 0)
	gp := pool.NewGRPCPool(time.Minute)
	dir := NewDirector(sel, cm, gp)

	srv, err := New("127.0.0.1:0", dir)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Start(context.Background()) }()

	frontConn, err := grpc.NewClient(
		srv.Addr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}

	return &rig{front: srv, frontConn: frontConn, archive: a, shard: s, pool: gp}
}

// callHealth sends a Check() with optional x-cosmos-block-height metadata.
func callHealth(t *testing.T, conn *grpc.ClientConn, height string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if height != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, height)
	}
	_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	return err
}

func callOpaque(t *testing.T, conn *grpc.ClientConn, method string, payload []byte, headerHeight string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if headerHeight != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, headerHeight)
	}
	req := &emptypb.Empty{}
	req.ProtoReflect().SetUnknown(append([]byte(nil), payload...))
	return conn.Invoke(ctx, method, req, &emptypb.Empty{})
}

func heightPayload(field protowire.Number, height uint64) []byte {
	payload := protowire.AppendTag(nil, field, protowire.VarintType)
	return protowire.AppendVarint(payload, height)
}

func bodyPayload(method string, field protowire.Number, height uint64) []byte {
	if method != "/cosmos.base.tendermint.v1beta1.Service/ABCIQuery" {
		return heightPayload(field, height)
	}
	payload := protowire.AppendTag(nil, 1, protowire.BytesType)
	payload = protowire.AppendBytes(payload, []byte{0xde, 0xad, 0xbe, 0xef})
	payload = protowire.AppendTag(payload, 2, protowire.BytesType)
	payload = protowire.AppendString(payload, "/store/bank/key")
	return append(payload, heightPayload(field, height)...)
}

func TestGRPCRoutesByHeightHeader(t *testing.T) {
	r := setupGRPC(t)
	defer r.close()

	// Height 12345 is in shard1's coverage; archive is also eligible but
	// shard1 is more specific.
	if err := callHealth(t, r.frontConn, "12345"); err != nil {
		t.Fatalf("health check: %v", err)
	}
	if r.shard.hits.Load() != 1 {
		t.Errorf("expected shard1 hit; got archive=%d shard1=%d", r.archive.hits.Load(), r.shard.hits.Load())
	}
	if r.archive.hits.Load() != 0 {
		t.Errorf("expected no archive hit; got %d", r.archive.hits.Load())
	}
}

func TestGRPCFallsBackToArchiveOutsideRange(t *testing.T) {
	r := setupGRPC(t)
	defer r.close()

	if err := callHealth(t, r.frontConn, "90000"); err != nil {
		t.Fatalf("health check: %v", err)
	}
	if r.archive.hits.Load() != 1 {
		t.Errorf("expected archive hit; got %d", r.archive.hits.Load())
	}
}

func TestGRPCRoutesByRequestBodyHeight(t *testing.T) {
	for method, spec := range expectedBodyHeightMethods {
		t.Run(method, func(t *testing.T) {
			r := setupGRPC(t)
			defer r.close()

			payload := bodyPayload(method, spec.HeightField, 12345)
			if err := callOpaque(t, r.frontConn, method, payload, ""); err != nil {
				t.Fatalf("body-routed call: %v", err)
			}
			if r.shard.hits.Load() != 1 || r.archive.hits.Load() != 0 {
				t.Fatalf("expected shard1 only; archive=%d shard1=%d", r.archive.hits.Load(), r.shard.hits.Load())
			}
			got, _ := r.shard.body.Load().([]byte)
			if !bytes.Equal(got, payload) {
				t.Fatalf("upstream body changed: got %x want %x", got, payload)
			}
			gotHeader, _ := r.shard.heightHeader.Load().(string)
			if gotHeader != "12345" {
				t.Fatalf("derived upstream height metadata: got %q want %q", gotHeader, "12345")
			}
		})
	}
}

func TestGRPCRequestBodyHeightFallsBackToArchive(t *testing.T) {
	r := setupGRPC(t)
	defer r.close()

	method := "/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight"
	if err := callOpaque(t, r.frontConn, method, heightPayload(1, 90000), ""); err != nil {
		t.Fatalf("body-routed call: %v", err)
	}
	if r.archive.hits.Load() != 1 || r.shard.hits.Load() != 0 {
		t.Fatalf("expected archive only; archive=%d shard1=%d", r.archive.hits.Load(), r.shard.hits.Load())
	}
}

func TestGRPCUnknownBodyFieldRemainsLatestAndTransparent(t *testing.T) {
	r := setupGRPC(t)
	defer r.close()

	method := "/custom.query.v1.Query/LooksHistorical"
	payload := heightPayload(1, 12345)
	if err := callOpaque(t, r.frontConn, method, payload, ""); err != nil {
		t.Fatalf("opaque call: %v", err)
	}
	// A latest request excludes shard1 because its bounded upper height is
	// behind the observed head. If tag 1 were guessed as a height, this would
	// incorrectly hit shard1 instead.
	if r.archive.hits.Load() != 1 || r.shard.hits.Load() != 0 {
		t.Fatalf("unknown method must remain latest; archive=%d shard1=%d", r.archive.hits.Load(), r.shard.hits.Load())
	}
	got, _ := r.archive.body.Load().([]byte)
	if !bytes.Equal(got, payload) {
		t.Fatalf("upstream body changed: got %x want %x", got, payload)
	}
}

func TestGRPCKnownBodyWithoutUsableHeightRemainsLatestAndTransparent(t *testing.T) {
	r := setupGRPC(t)
	defer r.close()

	method := "/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight"
	payload := protowire.AppendTag(nil, 1, protowire.BytesType)
	payload = protowire.AppendString(payload, "not-a-varint-height")
	if err := callOpaque(t, r.frontConn, method, payload, ""); err != nil {
		t.Fatalf("opaque call: %v", err)
	}
	if r.archive.hits.Load() != 1 || r.shard.hits.Load() != 0 {
		t.Fatalf("invalid body height must remain latest; archive=%d shard1=%d", r.archive.hits.Load(), r.shard.hits.Load())
	}
	got, _ := r.archive.body.Load().([]byte)
	if !bytes.Equal(got, payload) {
		t.Fatalf("upstream body changed: got %x want %x", got, payload)
	}
	if gotHeader, _ := r.archive.heightHeader.Load().(string); gotHeader != "" {
		t.Fatalf("unexpected derived height metadata %q", gotHeader)
	}
}

func TestGRPCHeightHeaderOverridesRequestBody(t *testing.T) {
	r := setupGRPC(t)
	defer r.close()

	method := "/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight"
	if err := callOpaque(t, r.frontConn, method, heightPayload(1, 12345), "90000"); err != nil {
		t.Fatalf("body-routed call: %v", err)
	}
	if r.archive.hits.Load() != 1 || r.shard.hits.Load() != 0 {
		t.Fatalf("header must win; archive=%d shard1=%d", r.archive.hits.Load(), r.shard.hits.Load())
	}
	if gotHeader, _ := r.archive.heightHeader.Load().(string); gotHeader != "90000" {
		t.Fatalf("upstream header changed: got %q want %q", gotHeader, "90000")
	}
}

func TestGRPCFailoverWhenChosenBackendDies(t *testing.T) {
	r := setupGRPC(t)
	defer r.close()

	r.shard.dead.Store(true)

	// We iterate a few times because gRPC dial caching means the first call
	// may still try shard once. The director then circuit-records the
	// failure, and subsequent calls route around it.
	var lastErr error
	for i := 0; i < 4; i++ {
		err := callHealth(t, r.frontConn, "12345")
		lastErr = err
		if err == nil {
			break
		}
	}
	if lastErr != nil {
		t.Fatalf("after retries, expected success on archive; got %v", lastErr)
	}
	if r.archive.hits.Load() == 0 {
		t.Errorf("expected archive to serve after shard1 failure; got 0 hits")
	}
}

func TestGRPCBroadcastNotIdempotent(t *testing.T) {
	// Build a route key for a known broadcast method; verify it's flagged
	// non-idempotent so the director wouldn't retry on partial failure.
	key := buildRouteKey("/cosmos.tx.v1beta1.Service/BroadcastTx", metadata.MD{}, nil)
	if key.Idempotent {
		t.Error("BroadcastTx must not be idempotent")
	}
	if key.Class != types.ClassBroadcast {
		t.Errorf("class: %s", key.Class)
	}
}

func TestGRPCRouteKeyHeightFromMetadata(t *testing.T) {
	md := metadata.New(map[string]string{HeightHeader: "777"})
	key := buildRouteKey("/cosmos.staking.v1beta1.Query/Validators", md, nil)
	if key.Class != types.ClassByHeight {
		t.Errorf("class: %s", key.Class)
	}
	if key.HeightOrZero() != 777 {
		t.Errorf("height: %d", key.HeightOrZero())
	}
}

func TestGRPCRouteKeyBodyHeightAndPrecedence(t *testing.T) {
	bodyHeight := int64(12345)
	key := buildRouteKey(
		"/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight",
		metadata.MD{},
		&bodyHeight,
	)
	if key.Class != types.ClassByHeight || key.HeightOrZero() != bodyHeight {
		t.Fatalf("body key: class=%s height=%d", key.Class, key.HeightOrZero())
	}

	md := metadata.New(map[string]string{HeightHeader: "90000"})
	key = buildRouteKey(
		"/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight",
		md,
		&bodyHeight,
	)
	if key.HeightOrZero() != 90000 {
		t.Fatalf("metadata must win over body: height=%d", key.HeightOrZero())
	}

	md = metadata.New(map[string]string{HeightHeader: "not-a-height"})
	key = buildRouteKey(
		"/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight",
		md,
		&bodyHeight,
	)
	if key.HeightOrZero() != bodyHeight {
		t.Fatalf("invalid metadata should fall back to body: height=%d", key.HeightOrZero())
	}

	key = buildRouteKey("/cosmos.tx.v1beta1.Service/BroadcastTx", md, &bodyHeight)
	if key.Class != types.ClassBroadcast || key.Height != nil {
		t.Fatalf("broadcast must ignore heights: class=%s height=%v", key.Class, key.Height)
	}
}

func TestExtractRequestHeightManifest(t *testing.T) {
	if len(Manifest) != len(expectedBodyHeightMethods) {
		t.Fatalf("manifest size: got %d want %d", len(Manifest), len(expectedBodyHeightMethods))
	}
	for method, want := range expectedBodyHeightMethods {
		t.Run(method, func(t *testing.T) {
			spec, ok := Manifest[method]
			if !ok {
				t.Fatal("method missing from manifest")
			}
			if spec != want {
				t.Fatalf("spec: got %+v want %+v", spec, want)
			}
			payload := protowire.AppendTag(nil, 99, protowire.BytesType)
			payload = protowire.AppendBytes(payload, []byte("opaque"))
			payload = append(payload, heightPayload(spec.HeightField, 180102279)...)

			got, ok := extractRequestHeight(method, payload)
			if !ok || got != 180102279 {
				t.Fatalf("height: got (%d, %t)", got, ok)
			}
		})
	}
}

func TestExtractRequestHeightLastValueWins(t *testing.T) {
	method := "/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight"
	payload := append(heightPayload(1, 11), heightPayload(1, 22)...)
	got, ok := extractRequestHeight(method, payload)
	if !ok || got != 22 {
		t.Fatalf("height: got (%d, %t), want (22, true)", got, ok)
	}

	for name, last := range map[string]uint64{
		"zero":     0,
		"negative": math.MaxUint64,
	} {
		t.Run(name, func(t *testing.T) {
			payload := append(heightPayload(1, 11), heightPayload(1, last)...)
			if got, ok := extractRequestHeight(method, payload); ok {
				t.Fatalf("last value must win; unexpected height %d", got)
			}
		})
	}
}

func TestExtractRequestHeightRejectsUnsafeValues(t *testing.T) {
	method := "/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight"
	tests := map[string][]byte{
		"zero":          heightPayload(1, 0),
		"negative":      heightPayload(1, math.MaxUint64),
		"above max int": heightPayload(1, uint64(math.MaxInt64)+1),
		"wrong field":   heightPayload(2, 12345),
		"wrong wire":    protowire.AppendString(protowire.AppendTag(nil, 1, protowire.BytesType), "12345"),
		"malformed":     {0x08, 0x80},
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			if got, ok := extractRequestHeight(method, payload); ok {
				t.Fatalf("unexpected height %d", got)
			}
		})
	}

	if got, ok := extractRequestHeight("/custom.Query/ByHeight", heightPayload(1, 12345)); ok {
		t.Fatalf("unknown method must not infer height %d", got)
	}
}

func TestManifestExcludesNonRoutingHeightLookalikes(t *testing.T) {
	methods := []string{
		"/cosmos.distribution.v1beta1.Query/ValidatorSlashes",
		"/cosmos.upgrade.v1beta1.Query/UpgradedConsensusState",
		"/ibc.core.client.v1.Query/ConsensusState",
		"/ibc.core.channel.v1.Query/PacketCommitment",
	}
	for _, method := range methods {
		if _, ok := Lookup(method); ok {
			t.Errorf("height-like method must not be body-routed: %s", method)
		}
	}
}

type scriptedServerStream struct {
	grpc.ServerStream
	payloads [][]byte
	receives int
}

func (s *scriptedServerStream) RecvMsg(dst any) error {
	if s.receives >= len(s.payloads) {
		return io.EOF
	}
	payload := s.payloads[s.receives]
	s.receives++
	msg, ok := dst.(proto.Message)
	if !ok {
		return io.ErrUnexpectedEOF
	}
	return proto.Unmarshal(payload, msg)
}

func TestSlotStreamReplaysFirstExactlyOnce(t *testing.T) {
	first := bodyPayload("/cosmos.base.tendermint.v1beta1.Service/ABCIQuery", 3, 12345)
	second := heightPayload(7, 67890)
	underlying := &scriptedServerStream{payloads: [][]byte{second}}
	stream := &slotStream{
		ServerStream: underlying,
		ctx:          context.Background(),
		first:        first,
		hasFirst:     true,
	}

	var gotFirst emptypb.Empty
	if err := stream.RecvMsg(&gotFirst); err != nil {
		t.Fatalf("first RecvMsg: %v", err)
	}
	if underlying.receives != 0 {
		t.Fatalf("first replay touched underlying stream %d time(s)", underlying.receives)
	}
	if !bytes.Equal(gotFirst.ProtoReflect().GetUnknown(), first) {
		t.Fatalf("first payload changed: got %x want %x", gotFirst.ProtoReflect().GetUnknown(), first)
	}
	if stream.first != nil {
		t.Fatal("replayed buffer was retained")
	}

	var gotSecond emptypb.Empty
	if err := stream.RecvMsg(&gotSecond); err != nil {
		t.Fatalf("second RecvMsg: %v", err)
	}
	if underlying.receives != 1 {
		t.Fatalf("second receive count: got %d want 1", underlying.receives)
	}
	if !bytes.Equal(gotSecond.ProtoReflect().GetUnknown(), second) {
		t.Fatalf("second payload changed: got %x want %x", gotSecond.ProtoReflect().GetUnknown(), second)
	}
}

func TestSlotStreamReplaysEmptyFirstFrame(t *testing.T) {
	second := heightPayload(7, 67890)
	underlying := &scriptedServerStream{payloads: [][]byte{second}}
	stream := &slotStream{
		ServerStream: underlying,
		ctx:          context.Background(),
		hasFirst:     true,
	}

	var got emptypb.Empty
	if err := stream.RecvMsg(&got); err != nil {
		t.Fatalf("empty replay: %v", err)
	}
	if underlying.receives != 0 {
		t.Fatalf("empty replay touched underlying stream %d time(s)", underlying.receives)
	}
	if len(got.ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("empty replay produced %x", got.ProtoReflect().GetUnknown())
	}
}

func TestGRPCPoolReusesConnections(t *testing.T) {
	a := newMockBackend(t, "archive")
	defer a.Stop()
	gp := pool.NewGRPCPool(time.Minute)
	defer gp.CloseAll()

	c1, err := gp.Conn(context.Background(), "archive", a.Addr())
	if err != nil {
		t.Fatal(err)
	}
	c2, err := gp.Conn(context.Background(), "archive", a.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if c1 != c2 {
		t.Error("expected pool to return same conn for same (backend, addr)")
	}
}

func TestGRPCPoolEvictsIdle(t *testing.T) {
	a := newMockBackend(t, "archive")
	defer a.Stop()
	gp := pool.NewGRPCPool(20 * time.Millisecond)
	defer gp.CloseAll()

	if _, err := gp.Conn(context.Background(), "archive", a.Addr()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	gp.EvictIdle()
	// After eviction, a fresh Conn should dial again — the pool should not
	// hold a closed reference.
	c, err := gp.Conn(context.Background(), "archive", a.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Error("expected fresh conn after eviction")
	}
}

// A half-open backend admits exactly one in-flight canary. The first
// Direct claims it via Acquire; a second Direct arriving before the canary
// resolves must skip to the next candidate instead of piling onto the
// probing backend. The legacy read-only Allow gate could not give this
// guarantee — it filtered, but consumed nothing. ReleaseOutcome (the
// client-vanished path) must free the slot without resolving the circuit.
func TestGRPCDirectorHalfOpenAdmitsSingleCanary(t *testing.T) {
	bs := []*backend.Backend{
		{
			Name:      "archive",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoGRPC: "127.0.0.1:19001"},
		},
		{
			Name:      "shard1",
			Coverage:  backend.Coverage{Kind: backend.CovBounded, Lower: 1, Upper: 50000},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoGRPC: "127.0.0.1:19002"},
		},
	}
	reg := backend.NewRegistry(bs)
	h := healthreg.NewRegistry()
	for _, bb := range bs {
		h.Update(healthreg.Snapshot{
			Backend: bb.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000,
		})
		h.Update(healthreg.Snapshot{
			Backend: bb.Name, Protocol: types.ProtoGRPC, Healthy: true,
		})
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   20 * time.Millisecond,
	})
	gp := pool.NewGRPCPool(time.Minute)
	defer gp.CloseAll()
	dir := NewDirector(selector.NewRangeSelector(reg, h, cm, 0), cm, gp)

	// Trip shard1's gRPC breaker, then let the cooldown elapse so the
	// selector readmits it as a (half-open-probe) candidate.
	cm.Record("shard1", types.ProtoGRPC, false)
	cm.Record("shard1", types.ProtoGRPC, false)
	time.Sleep(40 * time.Millisecond)

	// direct runs one routing decision with the post-call slot installed
	// (as streamHandler does) and reports the chosen backend. The pool's
	// gRPC dial is lazy, so no live upstream is needed.
	direct := func() string {
		t.Helper()
		slot := &atomicString{}
		ctx := context.WithValue(context.Background(), chosenBackendKey, slot)
		ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{HeightHeader: "12345"}))
		if _, _, err := dir.Direct(ctx, "/grpc.health.v1.Health/Check"); err != nil {
			t.Fatalf("Direct: %v", err)
		}
		return slot.Get()
	}

	// Height 12345 is in shard1's coverage, so shard1 outranks archive.
	if got := direct(); got != "shard1" {
		t.Fatalf("first Direct should claim the half-open shard1 canary; chose %q", got)
	}
	if got := direct(); got != "archive" {
		t.Fatalf("second Direct must skip shard1 while its canary is unresolved; chose %q", got)
	}

	// Client vanished: the admission is released without a sample, so the
	// canary slot is claimable again and the circuit state is untouched.
	dir.ReleaseOutcome("shard1")
	if got := direct(); got != "shard1" {
		t.Fatalf("after ReleaseOutcome the canary slot should be claimable again; chose %q", got)
	}
	if st := cm.State("shard1", types.ProtoGRPC); st != circuit.StateHalfOpen {
		t.Fatalf("ReleaseOutcome must not resolve the circuit; state %s", st)
	}

	// A real outcome resolves it: the canary success closes the breaker.
	dir.RecordOutcome("shard1", true)
	if st := cm.State("shard1", types.ProtoGRPC); st != circuit.StateClosed {
		t.Fatalf("canary success should close the breaker; state %s", st)
	}
}

// TestGRPCDeadlineExpiryRecordsFailure verifies that when a client context
// deadline fires while the backend is hanging, the circuit breaker records a
// FAILURE (not a neutral Release). Under the old wide predicate
// (ss.Context().Err() != nil), DeadlineExceeded would have triggered a
// Release, leaving the circuit closed even though the backend was too slow.
// With the fix, an expired gRPC deadline falls through to
// RecordOutcome(name, false) even if grpc-go exposes the stream context as
// context.Canceled.
//
// Red-first: if run against the old predicate, RecordOutcome(false) is never
// called; the breaker stays closed and the final Acquire returns true, causing
// the test to fail with "expected breaker to open after deadline expiry".
func TestGRPCDeadlineExpiryRecordsFailure(t *testing.T) {
	// Build a backend whose Check() blocks until its own context is done, so
	// it will always time out rather than return early.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hangSrv := grpc.NewServer()
	healthpb.RegisterHealthServer(hangSrv, &hangingHealthServer{})
	go func() { _ = hangSrv.Serve(lis) }()
	defer hangSrv.Stop()

	bs := []*backend.Backend{
		{
			Name:      "hang",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoGRPC: lis.Addr().String()},
		},
	}
	reg := backend.NewRegistry(bs)
	h := healthreg.NewRegistry()
	h.Update(healthreg.Snapshot{
		Backend: "hang", Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000,
	})
	h.Update(healthreg.Snapshot{
		Backend: "hang", Protocol: types.ProtoGRPC, Healthy: true,
	})

	// MinRequests=1 so a single failure is enough to trip the breaker.
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    1,
		OpenDuration:   5 * time.Second,
	})
	gp := pool.NewGRPCPool(time.Minute)
	defer gp.CloseAll()
	dir := NewDirector(selector.NewRangeSelector(reg, h, cm, 0), cm, gp)

	srv, err := New("127.0.0.1:0", dir)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Start(context.Background()) }()
	defer func() { _ = srv.Shutdown(context.Background()) }()

	frontConn, err := grpc.NewClient(
		srv.Addr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer frontConn.Close()

	// Make a call with a very short deadline so the backend times out.
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, _ = healthpb.NewHealthClient(frontConn).Check(ctx, &healthpb.HealthCheckRequest{Service: ""})

	// With the fix: DeadlineExceeded falls through to RecordOutcome(false),
	// and with MinRequests=1 the single failure trips the breaker open.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cm.State("hang", types.ProtoGRPC) == circuit.StateOpen {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st := cm.State("hang", types.ProtoGRPC); st != circuit.StateOpen {
		t.Fatalf("expected breaker to open after deadline expiry; state %s (neutral Release was used instead of RecordOutcome)", st)
	}
}

// hangingHealthServer is a gRPC health server whose Check blocks until the
// request context expires — simulating a backend that is too slow.
type hangingHealthServer struct {
	healthpb.UnimplementedHealthServer
}

func (h *hangingHealthServer) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// helpers --------------------------------------------------------------

func grpcUnavailable(msg string) error {
	// Use a generic error that the gRPC server will translate to
	// codes.Unknown — sufficient for the failover test.
	return &gErr{msg: msg}
}

type gErr struct{ msg string }

func (e *gErr) Error() string { return "rpc: " + e.msg }

// Sanity helper for IDE: ensure we reference these constants so the test
// file links cleanly even if the test names refactor.
var _ = strconv.Itoa
var _ = strings.HasPrefix
