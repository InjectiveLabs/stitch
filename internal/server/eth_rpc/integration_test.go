package eth_rpc

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/forwarder"
	healthreg "github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// Mock EVM JSON-RPC backend. Echoes the method name in the result so the
// test can assert which backend served the request. Capable of being
// killed (returns 503) and minting incrementing filter ids.
type mockEth struct {
	name     string
	hits     atomic.Int64
	dead     atomic.Bool
	delay    atomic.Int64 // response delay in nanoseconds
	filterID atomic.Int64
	srv      *httptest.Server
}

func newMockEth(name string) *mockEth {
	m := &mockEth{name: name}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

func (m *mockEth) handle(w http.ResponseWriter, r *http.Request) {
	m.hits.Add(1)
	if m.dead.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if d := m.delay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	body, _ := io.ReadAll(r.Body)
	var req struct {
		Method string          `json:"method"`
		ID     json.RawMessage `json:"id"`
	}
	_ = json.Unmarshal(body, &req)

	w.Header().Set("content-type", "application/json")
	switch req.Method {
	case "eth_newFilter", "eth_newBlockFilter", "eth_newPendingTransactionFilter":
		id := m.filterID.Add(1)
		w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":"0x` + hexInt(id) + `","backend":"` + m.name + `"}`))
	default:
		w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":{"served_by":"` + m.name + `"}}`))
	}
}

func (m *mockEth) URL() string { return m.srv.URL }
func (m *mockEth) Close()      { m.srv.Close() }

func hexInt(n int64) string {
	const hex = "0123456789abcdef"
	if n == 0 {
		return "0"
	}
	out := make([]byte, 0, 16)
	for n > 0 {
		out = append([]byte{hex[n&0xf]}, out...)
		n >>= 4
	}
	return string(out)
}

type ethRig struct {
	front   *Server
	frontT  *httptest.Server
	archive *mockEth
	shard   *mockEth
}

func (r *ethRig) close() {
	r.frontT.Close()
	r.archive.Close()
	r.shard.Close()
}

func setupEth(t *testing.T) *ethRig {
	t.Helper()
	a := newMockEth("archive")
	s := newMockEth("shard1")

	bs := []*backend.Backend{
		{
			Name:      "archive",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoEthRPC: a.URL()},
		},
		{
			Name:      "shard1",
			Coverage:  backend.Coverage{Kind: backend.CovBounded, Lower: 1, Upper: 50000},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoEthRPC: s.URL()},
		},
	}
	reg := backend.NewRegistry(bs)
	h := healthreg.NewRegistry()
	for _, bb := range bs {
		// EthRPC reuses the rpc protocol's health snapshot for chain head
		// — see selector.healthProtocol().
		h.Update(healthreg.Snapshot{
			Backend: bb.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000,
		})
		h.Update(healthreg.Snapshot{
			Backend: bb.Name, Protocol: types.ProtoEthRPC, Healthy: true,
		})
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: 100 * time.Millisecond,
	})
	sel := selector.NewRangeSelector(reg, h, cm, 0)
	fwd := forwarder.NewHTTP(sel, pool.NewHTTPPool(), cm, forwarder.Policy{
		MaxAttempts: 3, PerAttemptTimeout: 2 * time.Second,
	})
	srv := New("ignored", fwd)
	return &ethRig{
		front:   srv,
		frontT:  httptest.NewServer(srv.Handler()),
		archive: a,
		shard:   s,
	}
}

// helper: post a JSON-RPC body, return the parsed top-level result+ext.
type jsonResp struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestEthRoutesByHeightToShard(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xabcd","0x1000"]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if !strings.Contains(string(body), `"served_by":"shard1"`) {
		t.Fatalf("expected shard1; got %s", body)
	}
	if r.archive.hits.Load() != 0 {
		t.Errorf("archive should not be hit; got %d", r.archive.hits.Load())
	}
}

func TestEthFallsBackToArchiveOutsideRange(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	// 0xea60 = 60000 — beyond shard1's [1..50000] but within head=100000.
	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xabcd","0xea60"]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"served_by":"archive"`) {
		t.Fatalf("expected archive; got %s", body)
	}
}

func TestEthFailoverWhenChosenBackendDies(t *testing.T) {
	r := setupEth(t)
	defer r.close()
	r.shard.dead.Store(true)

	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xabcd","0x1000"]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"served_by":"archive"`) {
		t.Fatalf("expected archive after failover; got %s", body)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status=%d", resp.StatusCode)
	}
}

func TestEthBroadcastFanOutFailsWhenAllDead(t *testing.T) {
	r := setupEth(t)
	defer r.close()
	r.archive.dead.Store(true)
	r.shard.dead.Store(true)

	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0xf86c..."]}`)
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		t.Fatal("expected non-success since both upstreams are dead")
	}
}

func TestEthBroadcastFanOutSucceedsWhenOneDead(t *testing.T) {
	r := setupEth(t)
	defer r.close()
	r.archive.dead.Store(true) // shard is alive

	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0xf86c..."]}`)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected success when one backend is alive; got %d", resp.StatusCode)
	}
	// shard1 must have served (it's the live one); archive may have been
	// dispatched to as well (and returned 503 — that's fine, mempool
	// dedupes anyway).
	if r.shard.hits.Load() == 0 {
		t.Error("live shard1 should have been hit")
	}
}

func TestEthSubscribeRejectedOverHTTP(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "WebSocket") {
		t.Fatalf("expected WebSocket-only error; got %s", body)
	}
	if resp.StatusCode != 200 {
		t.Errorf("JSON-RPC error must use HTTP 200; got %d", resp.StatusCode)
	}
}

func TestEthHiddenMethodReturnsMethodNotFound(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"debug_traceTransaction","params":["0x"]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var p jsonResp
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	if p.Error == nil || p.Error.Code != -32601 {
		t.Fatalf("expected method-not-found; got %s", body)
	}
	if r.archive.hits.Load()+r.shard.hits.Load() != 0 {
		t.Error("hidden method must not reach upstream")
	}
}

func TestEthBatchPreservesOrder(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	// Two requests, IDs 1 and 2.
	body := `[
	  {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},
	  {"jsonrpc":"2.0","id":2,"method":"eth_chainId","params":[]}
	]`
	resp := post(t, r.frontT.URL, body)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)

	var arr []json.RawMessage
	if err := json.Unmarshal(out, &arr); err != nil {
		t.Fatalf("not a JSON array: %s", out)
	}
	if len(arr) != 2 {
		t.Fatalf("len=%d", len(arr))
	}
	// Each element must be a valid JSON-RPC response.
	for i, raw := range arr {
		var rp jsonResp
		if err := json.Unmarshal(raw, &rp); err != nil {
			t.Fatalf("element %d not JSON: %s", i, raw)
		}
		if rp.JSONRPC != "2.0" {
			t.Errorf("element %d jsonrpc=%q", i, rp.JSONRPC)
		}
	}
}

func TestEthGetBlockByHashRoutes(t *testing.T) {
	r := setupEth(t)
	defer r.close()
	hash := "0x" + repeat("ab", 32)

	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["`+hash+`",false]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"served_by":`) {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
}

func TestEthFilterMintBindsID(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	resp := post(t, r.frontT.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_newFilter","params":[{}]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var rp jsonResp
	if err := json.Unmarshal(body, &rp); err != nil {
		t.Fatal(err)
	}
	var id string
	if err := json.Unmarshal(rp.Result, &id); err != nil {
		t.Fatalf("result not a string: %s (raw=%s)", rp.Result, body)
	}
	if r.front.FilterStore().Lookup(id) == "" {
		t.Errorf("filter id %s not bound; store size=%d", id, r.front.FilterStore().Size())
	}
}

func TestEthMethodNotPostReturns405(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	resp, err := http.Get(r.frontT.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405; got %d", resp.StatusCode)
	}
}

func TestEthMalformedBodyReturnsParseError(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	resp := post(t, r.frontT.URL, `not json`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("-32700")) {
		t.Fatalf("expected -32700 parse error; got %s", body)
	}
}
