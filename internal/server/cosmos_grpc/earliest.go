package cosmos_grpc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/config"
	"github.com/InjectiveLabs/stitch/internal/metrics"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/types"
)

const (
	EarliestHeader           = "stitch"
	EarliestCapabilityHeader = "x-stitch-earliest-capability"
	EarliestHeightHeader     = "x-stitch-earliest-height"
	ArchiveChainIDHeader     = "x-stitch-cosmos-chain-id"
	BackendHeader            = "x-stitch-backend"
	earliestParamsMethod     = "/injective.evm.v1.Query/Params"
	archiveIdentityMethod    = "/cosmos.base.tendermint.v1beta1.Service/GetNodeInfo"
	earliestTimeout          = 30 * time.Second
	earliestAttemptTimeout   = 3 * time.Second
)

// Marked requests without dependent block inputs can be resolved directly.
// Execution/tracing resolve the profile before constructing their requests.
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

func historicalUnary(method string) bool {
	if earliestMethods[method] {
		return true
	}
	switch method {
	case "/injective.evm.v1.Query/EthCall", "/injective.evm.v1.Query/EstimateGas",
		"/injective.evm.v1.Query/TraceCall", "/injective.evm.v1.Query/TraceTx",
		"/injective.evm.v1.Query/TraceBlock", "/injective.evm.v1.Query/ValidatorAccount",
		"/cosmos.auth.v1beta1.Query/Params", "/cosmos.bank.v1beta1.Query/Params":
		return true
	default:
		return false
	}
}

type earliestReply struct {
	payload  []byte
	header   metadata.MD
	trailer  metadata.MD
	err      error
	admitted bool
}

func (d *Director) archiveProfile() (config.ArchiveProfile, bool) {
	if profiles, ok := d.selector.(interface {
		ArchiveProfile() (config.ArchiveProfile, bool)
	}); ok {
		return profiles.ArchiveProfile()
	}
	return config.ArchiveProfile{}, false
}

func (d *Director) serveEarliest(ss grpc.ServerStream, md metadata.MD) error {
	values := md.Get(EarliestHeader)
	if len(values) != 1 || (values[0] != "earliest" && values[0] != "archive-profile") {
		return status.Error(codes.InvalidArgument, "stitch metadata must be earliest or archive-profile")
	}
	method, ok := grpc.MethodFromServerStream(ss)
	if !ok || !earliestMethods[method] || (values[0] == "archive-profile" && method != earliestParamsMethod) {
		return status.Error(codes.Unimplemented, "resolve the archive profile before constructing this request")
	}
	if caps := md.Get(EarliestCapabilityHeader); len(caps) > 0 && (len(caps) != 1 || !validEarliestCapability(caps[0])) {
		return status.Error(codes.InvalidArgument, "invalid earliest capability")
	}
	if len(md.Get(BackendHeader)) != 0 {
		return status.Error(codes.InvalidArgument, "archive resolution cannot be pinned to a backend")
	}
	profile, configured := d.archiveProfile()
	if !configured {
		return status.Error(codes.FailedPrecondition, "earliest routing requires archive.evm_start_height and archive.cosmos_chain_id")
	}
	payload, err := receiveUnary(ss)
	if err != nil {
		return err
	}
	reply := earliestReply{header: metadata.MD{}}
	if values[0] == "earliest" {
		ctx, cancel := context.WithTimeout(ss.Context(), earliestTimeout)
		defer cancel()
		reply, err = d.replayHistorical(ctx, method, payload, md, profile.EVMStartHeight, &profile)
		if err != nil {
			return err
		}
	}
	// Every capability identifies the same logical block. Capability/state
	// retention never changes this declaration or demands a common backend
	// for later block, execution, proof, and parent-state reads.
	if reply.header == nil {
		reply.header = metadata.MD{}
	}
	height := strconv.FormatInt(profile.EVMStartHeight, 10)
	reply.header.Set(HeightHeader, height)
	reply.header.Set(EarliestHeightHeader, height)
	reply.header.Set(ArchiveChainIDHeader, profile.CosmosChainID)
	return sendUnary(ss, reply)
}

func (d *Director) serveHistorical(ss grpc.ServerStream, method string, md metadata.MD, height int64) error {
	payload, err := receiveUnary(ss)
	if err != nil {
		return err
	}
	var profile *config.ArchiveProfile
	if configured, ok := d.archiveProfile(); ok {
		profile = &configured
	}
	ctx, cancel := context.WithTimeout(ss.Context(), earliestTimeout)
	defer cancel()
	reply, err := d.replayHistorical(ctx, method, payload, md, height, profile)
	if err != nil {
		return err
	}
	return sendUnary(ss, reply)
}

func receiveUnary(ss grpc.ServerStream) ([]byte, error) {
	var request emptypb.Empty
	if err := ss.RecvMsg(&request); err != nil {
		return nil, err
	}
	if len(request.ProtoReflect().GetUnknown()) > 64*1024*1024 {
		return nil, status.Error(codes.ResourceExhausted, "historical request exceeds 64 MiB")
	}
	return append([]byte(nil), request.ProtoReflect().GetUnknown()...), nil
}

func sendUnary(ss grpc.ServerStream, reply earliestReply) error {
	if err := ss.SendHeader(reply.header); err != nil {
		return err
	}
	ss.SetTrailer(reply.trailer)
	if reply.err != nil {
		return reply.err
	}
	var response emptypb.Empty
	if err := proto.Unmarshal(reply.payload, &response); err != nil {
		return status.Error(codes.Internal, "invalid selected protobuf response")
	}
	return ss.SendMsg(&response)
}

// replayHistorical buffers each actual unary response before committing. A
// failed backend can be retried only at the same height, including a trace's
// explicitly requested parent height. Application outcomes never advance H.
func (d *Director) replayHistorical(ctx context.Context, method string, payload []byte, md metadata.MD, height int64, profile *config.ArchiveProfile) (earliestReply, error) {
	key := types.RouteKey{Protocol: types.ProtoGRPC, Method: method, Class: types.ClassByHeight, Height: &height, Idempotent: true}
	if names := md.Get(BackendHeader); len(names) > 0 {
		if len(names) != 1 || names[0] == "" || strings.ContainsAny(names[0], " ,\t\r\n") {
			return earliestReply{}, status.Error(codes.InvalidArgument, "backend affinity requires one backend name")
		}
		key.Backend = names[0]
	}
	candidates := d.selector.Candidates(key)
	if len(candidates) > 256 {
		return earliestReply{}, status.Error(codes.ResourceExhausted, "historical replay exceeds 256 candidates")
	}
	var lastErr error
	for _, b := range candidates {
		if err := ctx.Err(); err != nil {
			return earliestReply{}, status.FromContextError(err).Err()
		}
		if profile != nil {
			if err := d.verifyArchiveIdentity(ctx, b, md, profile.CosmosChainID); err != nil {
				lastErr = err
				continue
			}
		}
		reply := d.earliestInvoke(ctx, b, method, payload, md, height)
		if retryHistorical(reply.err) {
			d.finishHistorical(ctx, b, &reply, nil)
			lastErr = reply.err
			continue
		}
		if reply.err == nil {
			if _, err := exactResponseHeight(reply.header, height); err != nil {
				d.finishHistorical(ctx, b, &reply, err)
				lastErr = err
				continue
			}
		} else {
			// Domain errors may omit BaseApp's height header. Validate the
			// snapshot independently before interpreting absence or a revert.
			if len(reply.header.Get(HeightHeader)) != 0 {
				if _, err := exactResponseHeight(reply.header, height); err != nil {
					d.finishHistorical(ctx, b, &reply, err)
					lastErr = err
					continue
				}
			}
			// Release a half-open domain outcome before the witness claims
			// its own canary admission on this same backend.
			d.finishHistorical(ctx, b, &reply, nil)
			if err := d.verifyHistoricalSnapshot(ctx, b, md, method, height); err != nil {
				d.finishHistorical(ctx, b, &reply, nil)
				lastErr = err
				continue
			}
		}
		d.finishHistorical(ctx, b, &reply, nil)
		if reply.header == nil {
			reply.header = metadata.MD{}
		}
		reply.header.Set(HeightHeader, strconv.FormatInt(height, 10))
		// Diagnostic/legacy affinity only. New gateways do not pin later
		// reads: every dependency independently verifies the same snapshot.
		reply.header.Set(BackendHeader, b.Name)
		metrics.RequestsTotal.WithLabelValues(string(types.ProtoGRPC), key.Class.String(), b.Name, "replayed").Inc()
		return reply, nil
	}
	if err := ctx.Err(); err != nil {
		return earliestReply{}, status.FromContextError(err).Err()
	}
	if lastErr != nil {
		return earliestReply{}, status.Errorf(codes.Unavailable, "historical request unavailable at height %d: %v", height, lastErr)
	}
	return earliestReply{}, status.Errorf(codes.Unavailable, "no eligible backend for historical height %d", height)
}

func retryHistorical(err error) bool {
	if err == nil {
		return false
	}
	code := status.Code(err)
	message := strings.ToLower(status.Convert(err).Message())
	decodeFailure := code == codes.DataLoss || (code == codes.Internal && (strings.Contains(message, "unmarshal") || strings.Contains(message, "failed to decode") || strings.Contains(message, "malformed")))
	return missingEarliestHistory(err) || code == codes.Unavailable || code == codes.DeadlineExceeded || code == codes.Unimplemented || decodeFailure
}

func (d *Director) verifyHistoricalSnapshot(ctx context.Context, b *backend.Backend, md metadata.MD, method string, height int64) (err error) {
	witness := earliestParamsMethod
	if strings.HasPrefix(method, "/cosmos.auth.") {
		witness = "/cosmos.auth.v1beta1.Query/Params"
	}
	if strings.HasPrefix(method, "/cosmos.bank.") {
		witness = "/cosmos.bank.v1beta1.Query/Params"
	}
	reply := d.earliestInvoke(ctx, b, witness, nil, md, height)
	defer func() {
		var validation error
		if reply.err == nil {
			validation = err
		}
		d.finishHistorical(ctx, b, &reply, validation)
	}()
	if reply.err != nil {
		return reply.err
	}
	_, err = exactResponseHeight(reply.header, height)
	return err
}

// Chain identity is checked on the same gRPC endpoint, independently from
// historical state availability. This is not proof of block-hash equivalence
// at H; full archive parity remains a differential validation requirement.
func (d *Director) verifyArchiveIdentity(ctx context.Context, b *backend.Backend, md metadata.MD, expected string) (err error) {
	reply := d.earliestInvoke(ctx, b, archiveIdentityMethod, nil, md, 0)
	defer func() {
		var validation error
		if reply.err == nil {
			validation = err
		}
		d.finishHistorical(ctx, b, &reply, validation)
	}()
	if reply.err != nil {
		return reply.err
	}
	node, err := bytesField(reply.payload, 1)
	if err != nil {
		return err
	}
	network, err := bytesField(node, 4)
	if err != nil {
		return err
	}
	if string(network) != expected {
		return fmt.Errorf("upstream node identity does not match Cosmos chain %q", expected)
	}
	return nil
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
	budget := earliestAttemptTimeout
	switch method {
	case "/injective.evm.v1.Query/EthCall", "/injective.evm.v1.Query/EstimateGas", "/injective.evm.v1.Query/TraceTx", "/injective.evm.v1.Query/TraceBlock", "/injective.evm.v1.Query/TraceCall":
		budget = earliestTimeout // execution retains the enclosing request budget
	}
	callCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	out := md.Copy()
	for _, key := range []string{EarliestHeader, EarliestCapabilityHeader, EarliestHeightHeader, ArchiveChainIDHeader, BackendHeader, HeightHeader} {
		out.Delete(key)
	}
	if height > 0 {
		out.Set(HeightHeader, strconv.FormatInt(height, 10))
	}
	callCtx = metadata.NewOutgoingContext(callCtx, out)
	conn, err := d.pool.Conn(callCtx, b.Name, pool.CleanAddr(b.Endpoint(types.ProtoGRPC)))
	reply := earliestReply{admitted: true}
	if err == nil {
		var req, resp emptypb.Empty
		if err = proto.Unmarshal(payload, &req); err == nil {
			maxResponse := 64 * 1024 * 1024
			switch method {
			case earliestParamsMethod:
				maxResponse = 64 * 1024
			case archiveIdentityMethod:
				maxResponse = 1024 * 1024
			}
			err = conn.Invoke(callCtx, method, &req, &resp, grpc.Header(&reply.header), grpc.Trailer(&reply.trailer), grpc.MaxCallRecvMsgSize(maxResponse))
			reply.payload = append([]byte(nil), resp.ProtoReflect().GetUnknown()...)
		}
	}
	reply.err = err
	return reply
}

// Resolve each circuit admission only after height/identity validation. A
// transport-successful response from the wrong snapshot must count as failure.
func (d *Director) finishHistorical(ctx context.Context, b *backend.Backend, reply *earliestReply, validation error) {
	if !reply.admitted {
		return
	}
	reply.admitted = false
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		d.circuit.Release(b.Name, types.ProtoGRPC)
	case validation != nil:
		d.circuit.Record(b.Name, types.ProtoGRPC, false)
	case reply.err == nil:
		d.circuit.Record(b.Name, types.ProtoGRPC, true)
	case missingEarliestHistory(reply.err), status.Code(reply.err) == codes.Unimplemented, !retryHistorical(reply.err):
		d.circuit.Release(b.Name, types.ProtoGRPC)
	default:
		d.circuit.Record(b.Name, types.ProtoGRPC, false)
	}
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

func bytesField(payload []byte, field protowire.Number) ([]byte, error) {
	var value []byte
	for len(payload) > 0 {
		num, typ, n := protowire.ConsumeTag(payload)
		if n < 0 {
			return nil, errors.New("malformed protobuf")
		}
		payload = payload[n:]
		if num == field && typ == protowire.BytesType {
			v, m := protowire.ConsumeBytes(payload)
			if m < 0 {
				return nil, errors.New("malformed protobuf field")
			}
			value, payload = v, payload[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, payload)
		if m < 0 {
			return nil, errors.New("malformed protobuf value")
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
	return (strings.Contains(m, "failed to load state at height") && (strings.Contains(m, "version does not exist") || strings.Contains(m, "no versions found") || strings.Contains(m, "version mismatch"))) || (strings.Contains(m, "height") && strings.Contains(m, "is not available, lowest height is"))
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
