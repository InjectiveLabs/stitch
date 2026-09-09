package cosmos_grpc_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type capabilityCall struct {
	method string
	height int64
	path   string
	prove  bool
	data   string
}

type capabilityFixture struct {
	floors       map[string]int64
	wrongHeight  bool
	missingProof bool
	largeMethod  string
	largeBytes   int
	rawBlock     string
	rawABCI      string
	mu           sync.Mutex
	calls        []capabilityCall
}

func (f *capabilityFixture) start(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var request struct {
			Method string `json:"method"`
			Params struct {
				Height string `json:"height"`
				Path   string `json:"path"`
				Prove  bool   `json:"prove"`
				Data   string `json:"data"`
			} `json:"params"`
		}
		if err := json.NewDecoder(req.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		height, _ := strconv.ParseInt(request.Params.Height, 10, 64)
		f.mu.Lock()
		f.calls = append(f.calls, capabilityCall{method: request.Method, height: height, path: request.Params.Path, prove: request.Params.Prove, data: request.Params.Data})
		f.mu.Unlock()
		for _, key := range []string{"stitch", "x-stitch-backend", "x-stitch-earliest-capability"} {
			if req.Header.Get(key) != "" {
				t.Errorf("capability RPC leaked %s header", key)
			}
		}
		w.Header().Set("content-type", "application/json")
		if request.Method == "abci_query" {
			// Comet's HexBytes JSON decoder accepts bare hexadecimal, unlike
			// the URI interface or Ethereum quantities. In particular 0x03
			// must fail here, just as it does against a normal archival node.
			if _, err := hex.DecodeString(request.Params.Data); err != nil {
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "error": map[string]any{"code": -32602, "message": err.Error()}})
				return
			}
		}
		if request.Method == "block" && f.rawBlock != "" {
			_, _ = io.WriteString(w, f.rawBlock)
			return
		}
		if request.Method == "abci_query" && f.rawABCI != "" {
			_, _ = io.WriteString(w, f.rawABCI)
			return
		}
		if height < f.floors[request.Method] {
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "error": map[string]any{"code": -32603, "message": "Internal error", "data": fmt.Sprintf("height %d is not available, lowest height is %d", height, f.floors[request.Method])}})
			return
		}
		if f.wrongHeight {
			height++
		}
		h := strconv.FormatInt(height, 10)
		if f.largeMethod == request.Method && f.largeBytes > 0 {
			if request.Method == "block" {
				_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":{"block":{"header":{"height":%q},"data":{"txs":["`, h)
			} else {
				_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":{"height":%q,"txs_results":[{"events":[{"type":"fixture","attributes":[{"key":"payload","value":"`, h)
			}
			chunk := strings.Repeat("A", 32*1024)
			for remaining := f.largeBytes; remaining > 0; {
				n := min(remaining, len(chunk))
				if _, err := io.WriteString(w, chunk[:n]); err != nil {
					return
				}
				remaining -= n
			}
			if request.Method == "block" {
				_, _ = io.WriteString(w, `"]}}}}`)
			} else {
				_, _ = io.WriteString(w, `"}]}]}]}}`)
			}
			return
		}
		var result any
		switch request.Method {
		case "block":
			result = map[string]any{"block": map[string]any{"header": map[string]any{"height": h}}}
		case "block_results":
			result = map[string]any{"height": h, "txs_results": []any{}}
		case "abci_query":
			response := map[string]any{"code": 0, "height": h}
			if request.Params.Prove && !f.missingProof {
				response["proofOps"] = map[string]any{"ops": []any{map[string]any{"type": "ics23:iavl", "data": "AA=="}}}
			}
			result = map[string]any{"response": response}
		default:
			t.Errorf("unexpected capability method %s", request.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func (f *capabilityFixture) snapshot() []capabilityCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]capabilityCall(nil), f.calls...)
}

func TestEarliestCapabilityDiscoveryUsesRequiredHistory(t *testing.T) {
	for _, capability := range []string{"block", "execution", "trace", "proof", "storage", "range"} {
		t.Run(capability, func(t *testing.T) {
			comet := &capabilityFixture{floors: map[string]int64{"block": 128_000_007, "block_results": 128_000_019, "abci_query": 129_000_023}}
			shard := &historyShard{name: "different-history-floors", lower: 118_000_000, upper: 150_000_000, evmFloor: 127_000_000, rpcURL: comet.start(t)}
			r := newHistoryRig(t, shard)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			md := earliestMD()
			md.Set("x-stitch-earliest-capability", capability)
			_, headers, _, err := invokeHistory(ctx, r.conn, paramsMethod, md, nil)
			want := "128000019"
			if capability == "execution" {
				want = "128000007"
			}
			if capability == "proof" || capability == "storage" {
				want = "129000023"
			}
			if err != nil || firstValue(headers, resolvedKey) != want {
				t.Fatalf("capability floor not discovered: want=%s headers=%v err=%v", want, headers, err)
			}
			for _, call := range comet.snapshot() {
				if call.height < shard.lower || call.height > shard.upper {
					t.Errorf("capability lookup outside bounded shard: %+v", call)
				}
				if capability == "proof" && call.method == "abci_query" && !call.prove {
					t.Error("proof discovery did not request proof")
				}
			}
		})
	}
}

func TestEarliestTraceRequiresParentEVMState(t *testing.T) {
	comet := &capabilityFixture{}
	shard := &historyShard{name: "first-evm-block", lower: 118_000_000, upper: 150_000_000, evmFloor: 127_000_000, rpcURL: comet.start(t)}
	r := newHistoryRig(t, shard)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	md := earliestMD()
	md.Set("x-stitch-earliest-capability", "trace")
	_, headers, _, err := invokeHistory(ctx, r.conn, paramsMethod, md, nil)
	if err != nil || firstValue(headers, resolvedKey) != "127000001" {
		t.Fatalf("trace selected block whose parent EVM state is absent: headers=%v err=%v", headers, err)
	}
	if calls := comet.snapshot(); len(calls) != 2 || calls[0].height != 127_000_001 || calls[1].height != 127_000_001 {
		t.Errorf("trace fetched unnecessary large Comet responses after proving parent floor: %+v", calls)
	}
}

func TestEarliestCapabilityRejectsUnprovenResults(t *testing.T) {
	for _, invalid := range []string{"wrong-height", "missing-proof", "missing-rpc"} {
		t.Run(invalid, func(t *testing.T) {
			comet := &capabilityFixture{wrongHeight: invalid == "wrong-height", missingProof: invalid == "missing-proof"}
			shard := &historyShard{name: "incomplete-capability", lower: 127_000_000, upper: 127_000_010, evmFloor: 127_000_000}
			if invalid != "missing-rpc" {
				shard.rpcURL = comet.start(t)
			}
			r := newHistoryRig(t, shard)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			md := earliestMD()
			md.Set("x-stitch-earliest-capability", "proof")
			_, _, _, err := invokeHistory(ctx, r.conn, paramsMethod, md, nil)
			want := codes.FailedPrecondition
			if invalid == "wrong-height" {
				want = codes.Unavailable
			}
			if status.Code(err) != want {
				t.Errorf("unproven capability accepted: err=%v want=%v", err, want)
			}
		})
	}
}

func TestEarliestCapabilityUsesCometHexBytesWireEncoding(t *testing.T) {
	for _, capability := range []string{"proof", "storage"} {
		t.Run(capability, func(t *testing.T) {
			comet := &capabilityFixture{}
			shard := &historyShard{name: "strict-comet-json", lower: 127_000_000, upper: 130_000_000, evmFloor: 127_000_000, rpcURL: comet.start(t)}
			r := newHistoryRig(t, shard)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			md := earliestMD()
			md.Set("x-stitch-earliest-capability", capability)
			_, _, _, err := invokeHistory(ctx, r.conn, paramsMethod, md, nil)
			if err != nil {
				t.Fatalf("ordinary Comet HexBytes decoder rejected discovery request: %v", err)
			}
			want := map[string]string{"/store/evm/subspace": "02" + strings.Repeat("00", 20)}
			if capability == "proof" {
				want = map[string]string{"/store/evm/key": "03", "/store/acc/key": "01" + strings.Repeat("00", 20)}
			}
			for _, call := range comet.snapshot() {
				if call.method != "abci_query" {
					continue
				}
				if call.data != want[call.path] {
					t.Errorf("invalid Comet wire key for %s: %q", call.path, call.data)
				}
				delete(want, call.path)
			}
			if len(want) != 0 {
				t.Errorf("missing ABCI capability probes: %v", want)
			}
		})
	}
}

func TestEarliestCapabilityAvoidsFullBlockSearchWhenStateFloorIsUsable(t *testing.T) {
	comet := &capabilityFixture{}
	shard := &historyShard{name: "pre-evm-cosmos-range", lower: 118_000_000, upper: 150_000_000, evmFloor: 127_123_456, rpcURL: comet.start(t)}
	r := newHistoryRig(t, shard)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	md := earliestMD()
	md.Set("x-stitch-earliest-capability", "block")
	_, headers, _, err := invokeHistory(ctx, r.conn, paramsMethod, md, nil)
	if err != nil || firstValue(headers, resolvedKey) != "127123456" {
		t.Fatalf("state-floor-first discovery failed: headers=%v err=%v", headers, err)
	}
	calls := comet.snapshot()
	if len(calls) != 2 {
		t.Fatalf("fetched %d Comet payloads while discovering state; only block/results at resolved floor are needed", len(calls))
	}
	for _, call := range calls {
		if call.height != shard.evmFloor {
			t.Errorf("unnecessary large capability query at arbitrary state-search midpoint: %+v", call)
		}
	}
}

func TestEarliestCapabilityStreamsLargeLegitimateCometResults(t *testing.T) {
	for _, method := range []string{"block", "block_results"} {
		t.Run(method, func(t *testing.T) {
			comet := &capabilityFixture{largeMethod: method, largeBytes: 9 * 1024 * 1024}
			shard := &historyShard{name: "large-archive-response", lower: 127_000_000, upper: 127_000_001, evmFloor: 127_000_000, rpcURL: comet.start(t)}
			r := newHistoryRig(t, shard)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			md := earliestMD()
			md.Set("x-stitch-earliest-capability", "block")
			_, headers, _, err := invokeHistory(ctx, r.conn, paramsMethod, md, nil)
			if err != nil || firstValue(headers, resolvedKey) != "127000000" {
				t.Fatalf("legitimate >8MiB %s rejected: headers=%v err=%v", method, headers, err)
			}
		})
	}
}

func TestEarliestCapabilityRejectsMalformedCometProjectionInput(t *testing.T) {
	validPrefix := `{"jsonrpc":"2.0","id":1,"result":{"block":{"header":{"height":"127000000"}`
	tests := map[string]string{
		"truncated skipped transaction": validPrefix + `,"data":{"txs":["` + strings.Repeat("A", 64*1024),
		"invalid skipped string escape": validPrefix + `,"data":{"txs":["abc\q"]}}}}`,
		"trailing second JSON":          validPrefix + `}}} {"result":null}`,
		"trailing garbage":              validPrefix + `}}} trailing`,
		"wrong height":                  `{"jsonrpc":"2.0","id":1,"result":{"block":{"header":{"height":"127000001"}}}}`,
		"bad ignored number":            validPrefix + `,"ignored":01}}}`,
		"invalid ignored literal":       validPrefix + `,"ignored":fals}}}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			comet := &capabilityFixture{rawBlock: raw}
			shard := &historyShard{name: "malformed-comet", lower: 127_000_000, upper: 127_000_001, evmFloor: 127_000_000, rpcURL: comet.start(t)}
			r := newHistoryRig(t, shard)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			md := earliestMD()
			md.Set("x-stitch-earliest-capability", "block")
			_, _, _, err := invokeHistory(ctx, r.conn, paramsMethod, md, nil)
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("malformed/inconsistent response accepted as proven earliest: %v", err)
			}
		})
	}
}

func TestEarliestProofCapabilityRejectsWrongProofTypes(t *testing.T) {
	for _, ops := range []string{
		`"not an array"`, `{}`, `[null]`, `[42]`, `["not a proof"]`, `[{}]`,
		`[{"type":"ics23:iavl"}]`, `[{"type":"ics23:iavl","data":""}]`,
		`[{"type":null,"data":"AA=="}]`, `[{"type":"ics23:iavl","data":42}]`,
		`[{"type":"ics23:iavl","data":"AA=="},null]`,
	} {
		t.Run(ops, func(t *testing.T) {
			comet := &capabilityFixture{rawABCI: `{"jsonrpc":"2.0","id":1,"result":{"response":{"code":0,"height":"127000000","proofOps":{"ops":` + ops + `}}}}`}
			shard := &historyShard{name: "invalid-proof-type", lower: 127_000_000, upper: 127_000_000, evmFloor: 127_000_000, rpcURL: comet.start(t)}
			r := newHistoryRig(t, shard)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			md := earliestMD()
			md.Set("x-stitch-earliest-capability", "proof")
			_, _, _, err := invokeHistory(ctx, r.conn, paramsMethod, md, nil)
			if err == nil {
				t.Fatalf("invalid proof value %s established usable historical proofs", ops)
			}
		})
	}
}
