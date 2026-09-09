package cmt_rpc_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/cache"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/config"
	"github.com/InjectiveLabs/stitch/internal/forwarder"
	"github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/server/cmt_rpc"
	"github.com/InjectiveLabs/stitch/internal/types"
)

type archiveHTTPPeer struct {
	name, chain, mode string
	lower, upper      int64
	weight            int
	blockHeight       int64
	blockHash         string
	txs               []map[string]any
	totalOverride     string
	mu                sync.Mutex
	requests          []string
}

func (p *archiveHTTPPeer) handler(w http.ResponseWriter, r *http.Request) {
	method := strings.TrimPrefix(r.URL.Path, "/")
	replyID := json.RawMessage("1")
	params := map[string]any{}
	for key, values := range r.URL.Query() {
		if len(values) > 0 {
			params[key] = values[0]
		}
	}
	if r.Method == http.MethodPost {
		var envelope struct {
			Method string          `json:"method"`
			Params map[string]any  `json:"params"`
			ID     json.RawMessage `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		method, params = envelope.Method, envelope.Params
		replyID = envelope.ID
		if p.mode == "wrong-id" {
			replyID = json.RawMessage("999")
		}
	}
	p.mu.Lock()
	p.requests = append(p.requests, method+" "+fmt.Sprint(params))
	p.mu.Unlock()
	for _, key := range []string{"stitch", "x-stitch-backend", "x-stitch-earliest-capability"} {
		if r.Header.Get(key) != "" {
			http.Error(w, "private metadata leaked", 500)
			return
		}
	}
	reply := func(result any) {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": replyID, "result": result})
	}
	failure := func() {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": replyID, "error": map[string]any{"code": -32603, "message": "Internal error", "data": "failed to load state at height"}})
	}
	if method == "status" {
		reply(map[string]any{"node_info": map[string]any{"network": p.chain}, "sync_info": map[string]any{"latest_block_height": fmt.Sprint(p.upper)}})
		return
	}
	if p.mode == "unavailable" {
		failure()
		return
	}
	height, _ := strconv.ParseInt(fmt.Sprint(params["height"]), 10, 64)
	if p.mode == "wrong-height" {
		height++
	}
	switch method {
	case "block":
		reply(archiveHTTPBlock(height, p.chain, strings.Repeat("b7", 32)))
	case "block_results":
		reply(map[string]any{"height": fmt.Sprint(height), "txs_results": []any{}})
	case "abci_query":
		result := map[string]any{"height": fmt.Sprint(height), "code": 0, "value": fmt.Sprint(params["data"])}
		if p.mode == "empty-value" {
			result["value"] = ""
		}
		if fmt.Sprint(params["prove"]) == "true" && p.mode != "missing-proof" {
			op := map[string]any{"type": "ics23:iavl", "data": "AQ=="}
			if p.mode == "invalid-proof" {
				op["data"] = ""
			}
			result["proofOps"] = map[string]any{"ops": []any{op}}
		}
		reply(map[string]any{"response": result})
	case "block_by_hash":
		encodedHash := fmt.Sprint(params["hash"])
		wanted := strings.ToLower(strings.TrimPrefix(encodedHash, "0x"))
		if decoded, err := base64.StdEncoding.DecodeString(encodedHash); err == nil && len(decoded) == 32 {
			wanted = hex.EncodeToString(decoded)
		}
		if p.blockHeight > 0 && wanted == p.blockHash {
			reply(archiveHTTPBlock(p.blockHeight, p.chain, p.blockHash))
		} else {
			reply(map[string]any{"block": nil})
		}
	case "tx_search":
		total := fmt.Sprint(len(p.txs))
		if p.totalOverride != "" {
			total = p.totalOverride
		}
		txs := p.txs
		if txs == nil {
			txs = []map[string]any{}
		}
		reply(map[string]any{"txs": txs, "total_count": total})
	default:
		http.Error(w, "unexpected method "+method, 400)
	}
}

func archiveHTTPBlock(height int64, chain, hash string) map[string]any {
	return map[string]any{"block_id": map[string]any{"hash": hash}, "block": map[string]any{"header": map[string]any{"height": fmt.Sprint(height), "chain_id": chain}}}
}

type archiveHTTPRig struct {
	front    http.Handler
	registry *backend.Registry
	health   *health.Registry
}

func newArchiveHTTPRig(t *testing.T, start int64, chain string, peers ...*archiveHTTPPeer) archiveHTTPRig {
	t.Helper()
	h := health.NewRegistry()
	var bs []*backend.Backend
	var head int64
	for _, peer := range peers {
		server := httptest.NewServer(http.HandlerFunc(peer.handler))
		t.Cleanup(server.Close)
		weight := peer.weight
		if weight == 0 {
			weight = 100
		}
		bs = append(bs, &backend.Backend{Name: peer.name, Weight: weight, Coverage: backend.Coverage{Kind: backend.CovBounded, Lower: peer.lower, Upper: peer.upper}, Endpoints: map[types.Protocol]string{types.ProtoRPC: server.URL}})
		h.Update(health.Snapshot{Backend: peer.name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: peer.upper})
		head = max(head, peer.upper)
	}
	reg := backend.NewRegistry(bs)
	cm := circuit.NewManager(circuit.Policy{MinRequests: 1000, ErrorThreshold: .9, OpenDuration: time.Second})
	sel := selector.NewRangeSelector(reg, h, cm, 0)
	sel.SetArchiveProfile(&config.ArchiveProfile{EVMStartHeight: start, CosmosChainID: chain})
	hp := pool.NewHTTPPool()
	t.Cleanup(hp.CloseIdle)
	fwd := forwarder.NewHTTP(sel, hp, cm, forwarder.Policy{MaxAttempts: 3, PerAttemptTimeout: time.Second})
	server := cmt_rpc.New("127.0.0.1:0", fwd)
	server.SetResponseCache(cache.NewResponseCache(cache.ResponseCacheOpts{Capacity: 64}), func() int64 { return head }, 0, time.Minute)
	return archiveHTTPRig{front: server.Handler(), registry: reg, health: h}
}

func archiveHTTPGet(rig archiveHTTPRig, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	rig.front.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func archiveHTTPPost(rig archiveHTTPRig, method string, params any) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 73, "method": method, "params": params})
	w := httptest.NewRecorder()
	rig.front.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw)))
	return w
}

func TestArchiveHTTPExactHeightRetriesInvalidResponses(t *testing.T) {
	for _, mode := range []string{"unavailable", "wrong-height", "wrong-chain", "wrong-id"} {
		t.Run(mode, func(t *testing.T) {
			bad := &archiveHTTPPeer{name: "a-bad", chain: "archive-41", mode: mode, lower: 417, upper: 437, weight: 1000}
			if mode == "wrong-chain" {
				bad.chain = "another-chain"
			}
			good := &archiveHTTPPeer{name: "b-good", chain: "archive-41", lower: 417, upper: 437}
			rig := newArchiveHTTPRig(t, 417, "archive-41", bad, good)
			for run := 0; run < 2; run++ {
				var response *httptest.ResponseRecorder
				if mode == "wrong-id" {
					response = archiveHTTPPost(rig, "block", map[string]any{"height": "423"})
				} else {
					response = archiveHTTPGet(rig, "/block?height=423")
				}
				if response.Code != 200 || !strings.Contains(response.Body.String(), `"height":"423"`) || strings.Contains(response.Body.String(), "another-chain") {
					t.Fatalf("invalid response accepted/no same-height retry: %d %s", response.Code, response.Body.String())
				}
			}
			bad.mu.Lock()
			badCalls := len(bad.requests)
			bad.mu.Unlock()
			good.mu.Lock()
			goodCalls := len(good.requests)
			good.mu.Unlock()
			if badCalls == 0 || goodCalls == 0 {
				t.Fatalf("fixture did not exercise retry: bad=%d good=%d", badCalls, goodCalls)
			}
		})
	}
}

func TestArchiveHTTPGETABCIQueriesNeverShareDifferentParameterResults(t *testing.T) {
	peer := &archiveHTTPPeer{name: "archive", chain: "archive-43", lower: 61, upper: 83}
	rig := newArchiveHTTPRig(t, 61, "archive-43", peer)
	for _, data := range []string{"ABCD", "EF01", "ABCD"} {
		response := archiveHTTPGet(rig, "/abci_query?height=71&path=%22/store/evm/key%22&data="+data)
		if response.Code != 200 || !strings.Contains(response.Body.String(), `"value":"`+data+`"`) {
			t.Fatalf("GET params collided across repeated queries: %d %s", response.Code, response.Body.String())
		}
	}
}

func TestArchiveHTTPRequestedProofRetriesOnlyAtSameHeight(t *testing.T) {
	for _, mode := range []string{"missing-proof", "invalid-proof", "wrong-height"} {
		t.Run(mode, func(t *testing.T) {
			bad := &archiveHTTPPeer{name: "bad-proof", chain: "archive-45", mode: mode, lower: 911, upper: 933, weight: 1000}
			good := &archiveHTTPPeer{name: "good-proof", chain: "archive-45", lower: 911, upper: 933}
			rig := newArchiveHTTPRig(t, 911, "archive-45", bad, good)
			response := archiveHTTPGet(rig, "/abci_query?height=919&path=%22/store/evm/key%22&data=03&prove=true")
			if response.Code != 200 || !strings.Contains(response.Body.String(), `"height":"919"`) || !strings.Contains(response.Body.String(), `"data":"AQ=="`) {
				t.Fatalf("invalid proof was accepted or same-height replica not used: %d %s", response.Code, response.Body.String())
			}
			for _, peer := range []*archiveHTTPPeer{bad, good} {
				peer.mu.Lock()
				calls := append([]string(nil), peer.requests...)
				peer.mu.Unlock()
				queried := false
				for _, call := range calls {
					if strings.HasPrefix(call, "abci_query ") {
						queried = true
						if !strings.Contains(call, "height:919") {
							t.Errorf("proof retry changed H: %s", call)
						}
					}
				}
				if !queried {
					t.Errorf("proof scenario did not query %s", peer.name)
				}
			}
		})
	}
}

func TestArchiveHTTPEmptyABCIValueDoesNotSearchForLaterData(t *testing.T) {
	old := &archiveHTTPPeer{name: "empty-old", chain: "archive-46", mode: "empty-value", lower: 1201, upper: 1217}
	newer := &archiveHTTPPeer{name: "present-new", chain: "archive-46", lower: 1217, upper: 1231}
	rig := newArchiveHTTPRig(t, 1201, "archive-46", newer, old)
	response := archiveHTTPGet(rig, "/abci_query?height=1201&path=%22/store/evm/key%22&data=03")
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"value":""`) || !strings.Contains(response.Body.String(), `"height":"1201"`) {
		t.Fatalf("valid zero/empty state was lost: %d %s", response.Code, response.Body.String())
	}
	newer.mu.Lock()
	defer newer.mu.Unlock()
	if len(newer.requests) != 0 {
		t.Fatalf("empty value caused later-state search: %v", newer.requests)
	}
}

func TestArchiveHTTPHashSearchFindsOldShardDespiteNewerMiss(t *testing.T) {
	hash := strings.Repeat("71", 32)
	old := &archiveHTTPPeer{name: "old", chain: "archive-47", lower: 937, upper: 947, blockHeight: 939, blockHash: hash}
	newer := &archiveHTTPPeer{name: "new", chain: "archive-47", lower: 947, upper: 961}
	rig := newArchiveHTTPRig(t, 937, "archive-47", newer, old)
	decodedHash, err := hex.DecodeString(hash)
	if err != nil {
		t.Fatal(err)
	}
	// The real Comet Go client passes []byte, which its JSON codec encodes
	// as base64; hand-written hexadecimal requests alone miss this path.
	for _, encodedHash := range []string{"0x" + hash, base64.StdEncoding.EncodeToString(decodedHash)} {
		for run := 0; run < 2; run++ {
			response := archiveHTTPPost(rig, "block_by_hash", map[string]any{"hash": encodedHash})
			if response.Code != 200 || !strings.Contains(response.Body.String(), `"height":"939"`) || !strings.Contains(response.Body.String(), `"id":73`) {
				t.Fatalf("old hash %s lost or caller ID replaced: %d %s", encodedHash, response.Code, response.Body.String())
			}
		}
	}
}

func TestArchiveHTTPHashAbsenceRequiresCompleteHealthyCoverage(t *testing.T) {
	for _, condition := range []string{"complete", "unhealthy", "drained", "gap", "unavailable", "redundant-unhealthy"} {
		t.Run(condition, func(t *testing.T) {
			old := &archiveHTTPPeer{name: "old", chain: "archive-51", lower: 211, upper: 221}
			newer := &archiveHTTPPeer{name: "new", chain: "archive-51", lower: 221, upper: 239}
			peers := []*archiveHTTPPeer{old, newer}
			if condition == "gap" {
				newer.lower = 223
			}
			if condition == "unavailable" {
				old.mode = "unavailable"
			}
			if condition == "redundant-unhealthy" {
				peers = append(peers, &archiveHTTPPeer{name: "replica", chain: "archive-51", lower: 211, upper: 221})
			}
			rig := newArchiveHTTPRig(t, 211, "archive-51", peers...)
			if condition == "unhealthy" || condition == "redundant-unhealthy" {
				rig.health.Update(health.Snapshot{Backend: "old", Protocol: types.ProtoRPC, Healthy: false, LatestHeight: 221})
			}
			if condition == "drained" {
				rig.registry.Drain("old")
			}
			response := archiveHTTPGet(rig, "/block_by_hash?hash=0x"+strings.Repeat("05", 32))
			wantSuccess := condition == "complete" || condition == "redundant-unhealthy"
			if (response.Code == 200) != wantSuccess {
				t.Fatalf("coverage %s: %d %s", condition, response.Code, response.Body.String())
			}
			if wantSuccess && !strings.Contains(response.Body.String(), `"block":null`) {
				t.Fatalf("global miss shape changed: %s", response.Body.String())
			}
			if !wantSuccess && !strings.Contains(response.Body.String(), `"error"`) {
				t.Fatalf("incomplete search became null: %s", response.Body.String())
			}
		})
	}
}

func independentArchiveTx(height int64, index int, ethHash, payload string) map[string]any {
	raw := []byte(payload)
	sum := sha256.Sum256(raw)
	return map[string]any{"hash": hex.EncodeToString(sum[:]), "height": fmt.Sprint(height), "index": index, "tx": base64.StdEncoding.EncodeToString(raw), "tx_result": map[string]any{"code": 0, "events": []any{map[string]any{"type": "ethereum_tx", "attributes": []any{map[string]any{"key": "ethereumTxHash", "value": ethHash}}}}}}
}

func TestArchiveHTTPExactEthereumSearchDeduplicatesThenPaginates(t *testing.T) {
	ethHash := "0x" + strings.Repeat("82", 32)
	first := independentArchiveTx(327, 0, ethHash, "same overlap transaction")
	second := independentArchiveTx(329, 1, ethHash, "second transaction envelope")
	old := &archiveHTTPPeer{name: "old", chain: "archive-59", lower: 317, upper: 327, txs: []map[string]any{first}}
	newer := &archiveHTTPPeer{name: "new", chain: "archive-59", lower: 327, upper: 339, txs: []map[string]any{first, second}}
	rig := newArchiveHTTPRig(t, 317, "archive-59", old, newer)
	for _, tc := range []struct {
		page        int
		order, want string
	}{{1, "asc", "327"}, {2, "asc", "329"}, {1, "desc", "329"}} {
		response := archiveHTTPPost(rig, "tx_search", map[string]any{"query": "ethereum_tx.ethereumTxHash='" + ethHash + "'", "page": tc.page, "per_page": 1, "order_by": tc.order})
		var body struct {
			Result struct {
				Txs []struct {
					Height string `json:"height"`
				} `json:"txs"`
				Total string `json:"total_count"`
			} `json:"result"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if response.Code != 200 || body.Result.Total != "2" || len(body.Result.Txs) != 1 || body.Result.Txs[0].Height != tc.want {
			t.Fatalf("global dedupe/page failed: %d %s", response.Code, response.Body.String())
		}
	}
}

func TestArchiveHTTPExactEthereumSearchRejectsInvalidEvidence(t *testing.T) {
	for _, condition := range []string{"wrong-eth-hash", "wrong-comet-hash", "outside-shard", "truncated", "conflicting-replica"} {
		t.Run(condition, func(t *testing.T) {
			ethHash := "0x" + strings.Repeat("19", 32)
			tx := independentArchiveTx(451, 0, ethHash, "real independent bytes")
			peer := &archiveHTTPPeer{name: "archive", chain: "archive-61", lower: 443, upper: 467, txs: []map[string]any{tx}}
			peers := []*archiveHTTPPeer{peer}
			switch condition {
			case "wrong-eth-hash":
				peer.txs = []map[string]any{independentArchiveTx(451, 0, "0x"+strings.Repeat("20", 32), "different bytes")}
			case "wrong-comet-hash":
				tx["hash"] = strings.Repeat("00", 32)
			case "outside-shard":
				tx["height"] = "468"
			case "truncated":
				peer.totalOverride = "2"
			case "conflicting-replica":
				other := independentArchiveTx(452, 0, ethHash, "real independent bytes")
				peers = append(peers, &archiveHTTPPeer{name: "other", chain: "archive-61", lower: 443, upper: 467, txs: []map[string]any{other}})
			}
			rig := newArchiveHTTPRig(t, 443, "archive-61", peers...)
			response := archiveHTTPPost(rig, "tx_search", map[string]any{"query": "ethereum_tx.ethereumTxHash='" + ethHash + "'"})
			if response.Code == 200 || !strings.Contains(response.Body.String(), `"error"`) {
				t.Fatalf("invalid %s accepted: %d %s", condition, response.Code, response.Body.String())
			}
		})
	}
}
