// Package integration is the cross-package smoke test for Phase 1: it spins
// up two mock CometBFT upstreams, configures stitch programmatically with
// both, drives requests through the public listeners, and asserts routing,
// failover, and metrics behaviors end-to-end.
package integration

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/forwarder"
	"github.com/decentrio/stitch/internal/health"
	"github.com/decentrio/stitch/internal/pool"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/server/cmt_rpc"
	"github.com/decentrio/stitch/internal/server/cosmos_rest"
	"github.com/decentrio/stitch/internal/types"
)

type upstream struct {
	name   string
	hits   atomic.Int64
	dead   atomic.Bool
	height int64
	srv    *httptest.Server
}

func newUpstream(name string, height int64) *upstream {
	u := &upstream{name: name, height: height}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.hits.Add(1)
		if u.dead.Load() {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("content-type", "application/json")
		switch r.URL.Path {
		case "/status":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":-1,"result":{"sync_info":{"latest_block_height":"` + strconv.FormatInt(u.height, 10) + `","catching_up":false}}}`))
		case "/block":
			h := r.URL.Query().Get("height")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":-1,"result":{"block":{"header":{"height":"` + h + `"}},"served_by":"` + u.name + `"}}`))
		case "/block_by_hash":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":-1,"result":{"block":null,"served_by":"` + u.name + `"}}`))
		default:
			_, _ = w.Write([]byte(`{"served_by":"` + u.name + `"}`))
		}
	}))
	return u
}

func (u *upstream) URL() string { return u.srv.URL }
func (u *upstream) Close()      { u.srv.Close() }

type testRig struct {
	rpc     *httptest.Server
	rest    *httptest.Server
	archive *upstream
	shard   *upstream
}

func (r *testRig) close() {
	r.rpc.Close()
	r.rest.Close()
	r.archive.Close()
	r.shard.Close()
}

func setup(t *testing.T) *testRig {
	t.Helper()
	a := newUpstream("archive", 100000)
	s := newUpstream("shard1", 100000)

	bs := []*backend.Backend{
		{
			Name:      "archive",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoRPC: a.URL(), types.ProtoAPI: a.URL()},
		},
		{
			Name:      "shard1",
			Coverage:  backend.Coverage{Kind: backend.CovBounded, Lower: 1, Upper: 50000},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoRPC: s.URL(), types.ProtoAPI: s.URL()},
		},
	}
	reg := backend.NewRegistry(bs)
	h := health.NewRegistry()
	for _, bb := range bs {
		for _, p := range []types.Protocol{types.ProtoRPC, types.ProtoAPI} {
			h.Update(health.Snapshot{
				Backend: bb.Name, Protocol: p, Healthy: true, LatestHeight: 100000,
			})
		}
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    2,
		OpenDuration:   100 * time.Millisecond,
	})
	sel := selector.NewRangeSelector(reg, h, cm, 0)
	fwd := forwarder.NewHTTP(sel, pool.NewHTTPPool(), cm, forwarder.Policy{
		MaxAttempts:       3,
		PerAttemptTimeout: 2 * time.Second,
	})

	rpcS := cmt_rpc.New("ignored", fwd)
	restS := cosmos_rest.New("ignored", fwd)
	return &testRig{
		rpc:     httptest.NewServer(rpcS.Handler()),
		rest:    httptest.NewServer(restS.Handler()),
		archive: a,
		shard:   s,
	}
}

func TestRouteByHeightPrefersNarrowShard(t *testing.T) {
	rig := setup(t)
	defer rig.close()

	resp, err := http.Get(rig.rpc.URL + "/block?height=12345")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"served_by":"shard1"`) {
		t.Fatalf("expected shard1, got %s", body)
	}
	if rig.shard.hits.Load() != 1 || rig.archive.hits.Load() != 0 {
		t.Errorf("hits: archive=%d shard=%d", rig.archive.hits.Load(), rig.shard.hits.Load())
	}
}

func TestRouteByHeightFallsBackToArchive(t *testing.T) {
	rig := setup(t)
	defer rig.close()

	resp, err := http.Get(rig.rpc.URL + "/block?height=90000")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"served_by":"archive"`) {
		t.Fatalf("expected archive, got %s", body)
	}
}

func TestFailoverOnUpstream5xx(t *testing.T) {
	rig := setup(t)
	defer rig.close()
	rig.shard.dead.Store(true)

	resp, err := http.Get(rig.rpc.URL + "/block?height=12345")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("expected failover to succeed, got %d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"served_by":"archive"`) {
		t.Fatalf("expected archive after failover, got %s", body)
	}
}

func TestRESTHeightFromPath(t *testing.T) {
	rig := setup(t)
	defer rig.close()
	resp, err := http.Get(rig.rest.URL + "/cosmos/base/tendermint/v1beta1/blocks/1234")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"served_by":"shard1"`) {
		t.Fatalf("expected shard1, got %s", body)
	}
}

func TestRESTHeightFromHeader(t *testing.T) {
	rig := setup(t)
	defer rig.close()
	req, _ := http.NewRequest(http.MethodGet, rig.rest.URL+"/cosmos/auth/v1beta1/accounts/foo", nil)
	req.Header.Set("x-cosmos-block-height", "12345")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"served_by":"shard1"`) {
		t.Fatalf("expected shard1 via header, got %s", body)
	}
}

func TestBroadcastFanOutHitsAllHealthyBackends(t *testing.T) {
	rig := setup(t)
	defer rig.close()

	body := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"broadcast_tx_sync","params":{"tx":"AAA="}}`)
	resp, err := http.Post(rig.rpc.URL+"/", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200; got %d", resp.StatusCode)
	}
	// Both healthy backends should see the tx (fan-out by design).
	if rig.archive.hits.Load() == 0 || rig.shard.hits.Load() == 0 {
		t.Errorf("both backends should be hit by broadcast; archive=%d shard=%d",
			rig.archive.hits.Load(), rig.shard.hits.Load())
	}
}

func TestBroadcastFanOutFailsWhenAllDead(t *testing.T) {
	rig := setup(t)
	defer rig.close()
	rig.archive.dead.Store(true)
	rig.shard.dead.Store(true)

	body := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"broadcast_tx_sync","params":{"tx":"AAA="}}`)
	resp, err := http.Post(rig.rpc.URL+"/", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		t.Fatal("expected non-success since both upstreams are dead")
	}
}

func TestHashLookupRoutesAndSucceeds(t *testing.T) {
	rig := setup(t)
	defer rig.close()
	resp, err := http.Get(rig.rpc.URL + "/block_by_hash?hash=0xabcd")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
}

func TestJSONRPCEnvelopeWithObjectParams(t *testing.T) {
	rig := setup(t)
	defer rig.close()
	body := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"1234"}}`)
	resp, err := http.Post(rig.rpc.URL+"/", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var got map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if !strings.Contains(toJSON(got), "shard1") {
		t.Fatalf("expected shard1 in response: %s", toJSON(got))
	}
}

func TestCircuitOpensAfterRepeatedFailures(t *testing.T) {
	rig := setup(t)
	defer rig.close()
	rig.shard.dead.Store(true)

	// 4 idempotent calls — every one fails through shard, falls over to archive.
	for i := 0; i < 4; i++ {
		resp, err := http.Get(rig.rpc.URL + "/block?height=12345")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}

	// shard's circuit should be open by now; the next request should NOT
	// touch shard at all.
	preShardHits := rig.shard.hits.Load()
	preArchiveHits := rig.archive.hits.Load()
	resp, err := http.Get(rig.rpc.URL + "/block?height=12345")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if rig.shard.hits.Load() != preShardHits {
		t.Errorf("shard should be skipped (circuit open), but got %d new hits",
			rig.shard.hits.Load()-preShardHits)
	}
	if rig.archive.hits.Load() <= preArchiveHits {
		t.Error("archive should serve the request after shard is excluded")
	}
}

func toJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
