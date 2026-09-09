package cosmos_grpc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/metrics"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/types"
)

const (
	// EarliestHeader preserves the symbolic EVM selector across the gateway.
	EarliestHeader           = "stitch"
	EarliestCapabilityHeader = "x-stitch-earliest-capability"
	EarliestHeightHeader     = "x-stitch-earliest-height"
	BackendHeader            = "x-stitch-backend"
	earliestParamsMethod     = "/injective.evm.v1.Query/Params"
	earliestWorkers          = 4
	earliestTimeout          = 30 * time.Second
	earliestAttemptTimeout   = 3 * time.Second
)

// These unary state methods have no block identity embedded in their request
// body. Valid empty state and account absence are answers at the selected
// snapshot; only recognized unavailable-history outcomes advance discovery.
var earliestMethods = map[string]bool{
	earliestParamsMethod:                    true,
	"/cosmos.auth.v1beta1.Query/Account":    true,
	"/cosmos.bank.v1beta1.Query/Balance":    true,
	"/injective.evm.v1.Query/Account":       true,
	"/injective.evm.v1.Query/CosmosAccount": true,
	"/injective.evm.v1.Query/Balance":       true,
	"/injective.evm.v1.Query/Code":          true,
	"/injective.evm.v1.Query/Storage":       true,
	"/injective.evm.v1.Query/BaseFee":       true,
}

type earliestReply struct {
	payload []byte
	header  metadata.MD
	trailer metadata.MD
	err     error // A method-specific domain outcome, delivered with its metadata.
}

type earliestResult struct {
	backend *backend.Backend
	lower   int64
	height  int64
	reply   earliestReply
	err     error // Unresolved discovery, distinct from a valid domain response.
}

func (d *Director) serveEarliest(ss grpc.ServerStream, md metadata.MD) error {
	if values := md.Get(EarliestHeader); len(values) != 1 || values[0] != "earliest" {
		return status.Error(codes.InvalidArgument, "stitch metadata must be earliest")
	}
	method, ok := grpc.MethodFromServerStream(ss)
	if !ok || !earliestMethods[method] {
		return status.Error(codes.Unimplemented, "earliest discovery is not supported for this method; resolve EVM Params before constructing dependent requests")
	}
	capability := "state"
	if values := md.Get(EarliestCapabilityHeader); len(values) > 0 {
		if len(values) != 1 || !validEarliestCapability(values[0]) {
			return status.Error(codes.InvalidArgument, "invalid earliest capability")
		}
		capability = values[0]
	}
	if len(md.Get(BackendHeader)) > 0 {
		return status.Error(codes.InvalidArgument, "earliest discovery cannot be pinned to a backend")
	}
	var request emptypb.Empty
	if err := ss.RecvMsg(&request); err != nil {
		return err
	}
	payload := append([]byte(nil), request.ProtoReflect().GetUnknown()...)
	if len(payload) > 1024*1024 {
		return status.Error(codes.ResourceExhausted, "earliest request exceeds 1 MiB")
	}
	ctx, cancel := context.WithTimeout(ss.Context(), earliestTimeout)
	defer cancel()
	result, err := d.discoverEarliest(ctx, method, payload, md, capability)
	if err != nil {
		return err
	}
	header := result.reply.header.Copy()
	height := strconv.FormatInt(result.height, 10)
	header.Set(HeightHeader, height)
	header.Set(EarliestHeightHeader, height)
	header.Set(BackendHeader, result.backend.Name)
	if err := ss.SendHeader(header); err != nil {
		return err
	}
	ss.SetTrailer(result.reply.trailer)
	if result.reply.err != nil {
		return result.reply.err
	}
	var response emptypb.Empty
	if err := proto.Unmarshal(result.reply.payload, &response); err != nil {
		return status.Error(codes.Internal, "invalid selected protobuf response")
	}
	return ss.SendMsg(&response)
}

func (d *Director) discoverEarliest(ctx context.Context, method string, payload []byte, md metadata.MD, capability string) (earliestResult, error) {
	key := types.RouteKey{Protocol: types.ProtoGRPC, Method: method, Class: types.ClassEarliest, Idempotent: true}
	candidates := append([]*backend.Backend(nil), d.selector.Candidates(key)...)
	sort.SliceStable(candidates, func(i, j int) bool {
		li, lj := earliestLower(candidates[i]), earliestLower(candidates[j])
		if li != lj {
			return li < lj
		}
		if candidates[i].Weight != candidates[j].Weight {
			return candidates[i].Weight > candidates[j].Weight
		}
		return candidates[i].Name < candidates[j].Name
	})
	if len(candidates) > 256 {
		return earliestResult{}, status.Error(codes.ResourceExhausted, "earliest discovery exceeds 256 archive candidates")
	}
	eligible := make(map[string]bool, len(candidates))
	for _, b := range candidates {
		eligible[b.Name] = true
	}
	var results []earliestResult
	if scope, ok := d.selector.(interface{ EarliestUniverse() []*backend.Backend }); ok {
		for _, b := range scope.EarliestUniverse() {
			if !eligible[b.Name] {
				results = append(results, earliestResult{backend: b, lower: earliestLower(b), err: errors.New("backend health or circuit prevents discovery")})
			}
		}
	}
	jobs := make(chan *backend.Backend)
	done := make(chan earliestResult, earliestWorkers)
	var workers sync.WaitGroup
	for i := 0; i < min(earliestWorkers, len(candidates)); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for b := range jobs {
				done <- d.discoverBackend(ctx, b, method, payload, md, capability)
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, b := range candidates {
			select {
			case jobs <- b:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { workers.Wait(); close(done) }()
	winnerIndex := -1
	for result := range done {
		if result.err == nil && result.height > 0 && (winnerIndex < 0 || olderResult(result, results[winnerIndex])) {
			if winnerIndex >= 0 {
				results[winnerIndex].reply = earliestReply{}
			}
			winnerIndex = len(results)
		} else {
			// Keep only the winning payload; a shard may return up to 8 MiB.
			result.reply = earliestReply{}
		}
		results = append(results, result)
	}
	if err := ctx.Err(); err != nil {
		return earliestResult{}, status.FromContextError(err).Err()
	}
	var winner *earliestResult
	if winnerIndex >= 0 {
		winner = &results[winnerIndex]
	}
	// An unavailable older shard is not evidence of absent history. Even a
	// newer success cannot prove global earliest while that shard is unknown.
	for _, r := range results {
		if r.err != nil && (winner == nil || r.lower < winner.height) {
			metrics.RequestsTotal.WithLabelValues(string(types.ProtoGRPC), types.ClassEarliest.String(), r.backend.Name, "incomplete").Inc()
			return earliestResult{}, status.Errorf(codes.Unavailable, "earliest discovery incomplete: backend %s: %v", r.backend.Name, r.err)
		}
	}
	if winner == nil {
		return earliestResult{}, status.Error(codes.FailedPrecondition, "no usable EVM history for requested capability")
	}
	metrics.RequestsTotal.WithLabelValues(string(types.ProtoGRPC), types.ClassEarliest.String(), winner.backend.Name, "selected").Inc()
	return *winner, nil
}

func olderResult(a, b earliestResult) bool {
	return a.height < b.height || (a.height == b.height && (a.backend.Weight > b.backend.Weight || (a.backend.Weight == b.backend.Weight && a.backend.Name < b.backend.Name)))
}

func earliestLower(b *backend.Backend) int64 {
	if b.Coverage.Kind == backend.CovArchive {
		return 1
	}
	return max(1, b.Coverage.Lower)
}

func (d *Director) discoverBackend(ctx context.Context, b *backend.Backend, method string, payload []byte, md metadata.MD, capability string) earliestResult {
	result := earliestResult{backend: b, lower: earliestLower(b)}
	if b.Coverage.Kind == backend.CovPruned {
		return result
	}
	latest := d.earliestInvoke(ctx, b, earliestParamsMethod, nil, md, 0)
	if latest.err != nil {
		if status.Code(latest.err) == codes.Unimplemented {
			return result
		}
		result.err = latest.err
		return result
	}
	head, err := exactResponseHeight(latest.header, 0)
	if err != nil {
		result.err = err
		return result
	}
	upper := head
	if b.Coverage.Kind == backend.CovBounded {
		upper = min(upper, b.Coverage.Upper)
	}
	if upper < result.lower {
		return result
	}
	// The predicate is retained initialized EVM state (and required stores),
	// which has a contiguous availability window. It never tests whether an
	// arbitrary account/code/transaction result is nonempty. Application
	// outcomes are final at a usable snapshot and do not advance the floor.
	// Find initialized EVM state using the small Params response first. Only
	// then check the operation's block/results/proof requirements. Fetching
	// full blocks at every state-search midpoint is needlessly expensive.
	floor, reply, err := earliestFloor(result.lower, upper, func(height int64) (earliestReply, bool, error) {
		return d.earliestProbe(ctx, b, earliestParamsMethod, nil, md, "state", height)
	})
	if err != nil || floor == 0 {
		result.err = err
		return result
	}
	if capability == "trace" {
		// Stage one proved the first retained initialized state is floor.
		// A trace also needs its parent state, so floor+1 is the earliest
		// possible candidate. Avoid expensive midpoint block probes just
		// to rediscover this already-known parent boundary.
		if floor == upper {
			return result
		}
		floor++
	}
	if method != earliestParamsMethod || capability != "state" {
		floor, reply, err = earliestFloor(floor, upper, func(height int64) (earliestReply, bool, error) {
			return d.earliestProbe(ctx, b, method, payload, md, capability, height)
		})
	}
	result.height, result.reply, result.err = floor, reply, err
	return result
}

func earliestFloor(lower, upper int64, probe func(int64) (earliestReply, bool, error)) (int64, earliestReply, error) {
	reply, available, err := probe(lower)
	if err != nil {
		return 0, earliestReply{}, err
	}
	if available {
		return lower, reply, nil
	}
	if upper == lower {
		return 0, earliestReply{}, nil
	}
	reply, available, err = probe(upper)
	if err != nil || !available {
		return 0, earliestReply{}, err
	}
	low, high := lower+1, upper
	for low < high {
		mid := low + (high-low)/2
		next, valid, err := probe(mid)
		if err != nil {
			return 0, earliestReply{}, err
		}
		if valid {
			high, reply = mid, next
		} else {
			low = mid + 1
		}
	}
	return high, reply, nil
}

func (d *Director) earliestProbe(ctx context.Context, b *backend.Backend, method string, payload []byte, md metadata.MD, capability string, height int64) (earliestReply, bool, error) {
	params := d.earliestInvoke(ctx, b, earliestParamsMethod, nil, md, height)
	if params.err != nil {
		if missingEarliestHistory(params.err) || status.Code(params.err) == codes.Unimplemented {
			return earliestReply{}, false, nil
		}
		return earliestReply{}, false, params.err
	}
	if _, err := exactResponseHeight(params.header, height); err != nil {
		return earliestReply{}, false, err
	}
	initialized, err := initializedEVMParams(params.payload)
	if err != nil || !initialized {
		return earliestReply{}, false, err
	}
	if ok, err := d.earliestCapability(ctx, b, md, capability, height); !ok || err != nil {
		return earliestReply{}, false, err
	}
	if method == earliestParamsMethod {
		return params, true, nil
	}
	reply := d.earliestInvoke(ctx, b, method, payload, md, height)
	if reply.err != nil {
		if missingEarliestHistory(reply.err) || status.Code(reply.err) == codes.Unimplemented {
			return earliestReply{}, false, nil
		}
		if !earliestDomainOutcome(method, reply.err) {
			return earliestReply{}, false, reply.err
		}
	}
	// A successful Params read verifies the exact state even when BaseApp
	// omits height headers on a domain error such as an absent account.
	if reply.err == nil {
		if _, err := exactResponseHeight(reply.header, height); err != nil {
			return earliestReply{}, false, err
		}
	}
	return reply, true, nil
}

func (d *Director) earliestInvoke(ctx context.Context, b *backend.Backend, method string, payload []byte, md metadata.MD, height int64) earliestReply {
	select {
	case d.earliestSlots <- struct{}{}:
		defer func() { <-d.earliestSlots }()
	case <-ctx.Done():
		return earliestReply{err: status.FromContextError(ctx.Err()).Err()}
	}
	if !d.circuit.Acquire(b.Name, types.ProtoGRPC) {
		return earliestReply{err: status.Error(codes.Unavailable, "backend circuit is open")}
	}
	callCtx, cancel := context.WithTimeout(ctx, earliestAttemptTimeout)
	defer cancel()
	out := md.Copy()
	out.Delete(EarliestHeader)
	out.Delete(EarliestCapabilityHeader)
	out.Delete(EarliestHeightHeader)
	out.Delete(BackendHeader)
	out.Delete(HeightHeader)
	if height > 0 {
		out.Set(HeightHeader, strconv.FormatInt(height, 10))
	}
	callCtx = metadata.NewOutgoingContext(callCtx, out)
	conn, err := d.pool.Conn(callCtx, b.Name, pool.CleanAddr(b.Endpoint(types.ProtoGRPC)))
	reply := earliestReply{}
	if err == nil {
		var req, resp emptypb.Empty
		if err = proto.Unmarshal(payload, &req); err == nil {
			maxResponse := 8 * 1024 * 1024
			if method == earliestParamsMethod {
				maxResponse = 64 * 1024
			}
			err = conn.Invoke(callCtx, method, &req, &resp, grpc.Header(&reply.header), grpc.Trailer(&reply.trailer), grpc.MaxCallRecvMsgSize(maxResponse))
			reply.payload = append([]byte(nil), resp.ProtoReflect().GetUnknown()...)
		}
	}
	reply.err = err
	switch {
	case err == nil:
		d.circuit.Record(b.Name, types.ProtoGRPC, true)
	case missingEarliestHistory(err), earliestDomainOutcome(method, err), status.Code(err) == codes.Unimplemented:
		d.circuit.Release(b.Name, types.ProtoGRPC)
	case errors.Is(ctx.Err(), context.Canceled):
		d.circuit.Release(b.Name, types.ProtoGRPC)
	default:
		d.circuit.Record(b.Name, types.ProtoGRPC, false)
	}
	return reply
}

func exactResponseHeight(md metadata.MD, expected int64) (int64, error) {
	values := md.Get(HeightHeader)
	if len(values) != 1 {
		return 0, errors.New("upstream did not return one concrete Cosmos height")
	}
	height, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil || height <= 0 || (expected > 0 && height != expected) {
		return 0, fmt.Errorf("upstream Cosmos height %q does not match requested height %d", values[0], expected)
	}
	return height, nil
}

func initializedEVMParams(payload []byte) (bool, error) {
	params, err := bytesField(payload, 1)
	if err != nil {
		return false, err
	}
	denom, err := bytesField(params, 1)
	return len(denom) > 0, err
}

func bytesField(payload []byte, field protowire.Number) ([]byte, error) {
	var value []byte
	for len(payload) > 0 {
		num, typ, n := protowire.ConsumeTag(payload)
		if n < 0 {
			return nil, errors.New("malformed Params protobuf")
		}
		payload = payload[n:]
		if num == field && typ == protowire.BytesType {
			v, m := protowire.ConsumeBytes(payload)
			if m < 0 {
				return nil, errors.New("malformed Params protobuf field")
			}
			value, payload = v, payload[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, payload)
		if m < 0 {
			return nil, errors.New("malformed Params protobuf value")
		}
		payload = payload[m:]
	}
	return value, nil
}

func missingEarliestHistory(err error) bool {
	if err == nil {
		return false
	}
	m := strings.ToLower(status.Convert(err).Message())
	// SDK returns this as either InvalidArgument or Unknown depending on
	// version. Do not classify an arbitrary NotFound/Unknown as missing state.
	return (strings.Contains(m, "failed to load state at height") &&
		(strings.Contains(m, "version does not exist") || strings.Contains(m, "no versions found") || strings.Contains(m, "version mismatch"))) ||
		(strings.Contains(m, "height") && strings.Contains(m, "is not available, lowest height is"))
}

func earliestDomainOutcome(method string, err error) bool {
	if status.Code(err) != codes.NotFound {
		return false
	}
	switch method {
	case "/cosmos.auth.v1beta1.Query/Account", "/injective.evm.v1.Query/Account", "/injective.evm.v1.Query/CosmosAccount":
		return true
	default:
		return false
	}
}
