package cosmos_grpc_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestEarliestCapabilitiesShareLogicalHeight(t *testing.T) {
	const e int64 = 503
	var cometCalls atomic.Int64
	comet := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cometCalls.Add(1)
		http.Error(w, "state-only shard", http.StatusServiceUnavailable)
	}))
	defer comet.Close()
	shard := &historyShard{name: "state-only", lower: 1, upper: 1000, evmFloor: e, stateFloor: e, rpcURL: comet.URL}
	r := newHistoryRig(t, shard)
	r.archive(e)
	for _, capability := range []string{"state", "block", "execution", "trace", "proof", "storage", "range"} {
		md := earliestMD()
		md.Set("x-stitch-earliest-capability", capability)
		_, headers, _, err := invokeHistory(context.Background(), r.conn, paramsMethod, md, nil)
		if err != nil || firstValue(headers, resolvedKey) != strconv.FormatInt(e, 10) {
			t.Fatalf("%s changed the logical block: %v %v", capability, headers, err)
		}
	}
	if cometCalls.Load() != 0 {
		t.Fatal("profile resolution demanded co-located Comet history")
	}
	for _, call := range shard.snapshot() {
		if call.method == paramsMethod && call.height != e {
			t.Fatalf("capability probed a different height %d", call.height)
		}
	}
}

func TestArchiveProfileHandshakeIgnoresBackendAvailability(t *testing.T) {
	shard := &historyShard{name: "offline", lower: 1, upper: 1000, err: status.Error(codes.Unavailable, "down")}
	r := newHistoryRig(t, shard)
	r.archive(503)
	body, headers, _, err := invokeHistory(context.Background(), r.conn, paramsMethod, metadata.Pairs("stitch", "archive-profile"), nil)
	if err != nil || len(body) != 0 || firstValue(headers, resolvedKey) != "503" || firstValue(headers, heightKey) != "503" || firstValue(headers, "x-stitch-cosmos-chain-id") != "fixture-chain" {
		t.Fatalf("declaration handshake depended on history: %x %v %v", body, headers, err)
	}
	if len(shard.snapshot()) != 0 {
		t.Fatal("declaration handshake contacted an upstream")
	}
}

func TestTraceRetainsExplicitPreEVMParentSnapshot(t *testing.T) {
	const e int64 = 503
	shard := &historyShard{name: "parent-state", lower: 1, upper: 1000, evmFloor: e, payload: wireBytes(1, []byte("trace"))}
	r := newHistoryRig(t, shard)
	r.archive(e)
	// The logical trace block is E; its requested state is E-1. An empty EVM
	// Params value in that parent must not turn the trace into block E+1.
	payload := protowire.AppendVarint(protowire.AppendTag(nil, 5, protowire.VarintType), 503)
	body, headers, _, err := invokeHistory(context.Background(), r.conn, "/injective.evm.v1.Query/TraceBlock", metadata.Pairs(heightKey, "502"), payload)
	if err != nil || !bytes.Equal(body, shard.payload) || firstValue(headers, heightKey) != "502" {
		t.Fatalf("trace parent changed: %x %v %v", body, headers, err)
	}
	for _, call := range shard.snapshot() {
		if call.method == "/injective.evm.v1.Query/TraceBlock" && (call.height != 502 || !bytes.Equal(call.body, payload)) {
			t.Fatalf("trace inputs rewritten: %+v", call)
		}
	}
}

func TestEarliestRejectsInvalidMarkersAndUnsupportedMethods(t *testing.T) {
	shard := &historyShard{name: "unused", lower: 1, upper: 1000}
	r := newHistoryRig(t, shard)
	r.archive(503)
	for _, values := range [][]string{{"stitch", "bad"}, {"stitch", "earliest", "stitch", "earliest"}, {"stitch", "earliest", "x-stitch-earliest-capability", "unknown"}, {"stitch", "earliest", "x-stitch-backend", "unused"}} {
		_, _, _, err := invokeHistory(context.Background(), r.conn, paramsMethod, metadata.Pairs(values...), nil)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid marker accepted: %v", err)
		}
	}
	for _, method := range []string{"/cosmos.tx.v1beta1.Service/BroadcastTx", "/injective.evm.v1.Query/TraceBlock"} {
		_, _, _, err := invokeHistory(context.Background(), r.conn, method, earliestMD(), nil)
		if status.Code(err) != codes.Unimplemented {
			t.Fatalf("unsupported marked method accepted: %v", err)
		}
	}
	if len(shard.snapshot()) != 0 {
		t.Fatal("invalid requests reached upstream")
	}
}
