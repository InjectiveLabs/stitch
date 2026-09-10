package cmt_rpc_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/cache"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/forwarder"
	"github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/server/cmt_rpc"
	"github.com/InjectiveLabs/stitch/internal/types"
)

type affinityRig struct {
	front         http.Handler
	server        *cmt_rpc.Server
	reg           *backend.Registry
	health        *health.Registry
	circuit       *circuit.Manager
	hintedHits    atomic.Int64
	preferredHits atomic.Int64
	leaked        atomic.Bool
}

func newAffinityRig(t *testing.T) *affinityRig {
	t.Helper()
	r := &affinityRig{}
	hinted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.hintedHits.Add(1)
		for _, header := range []string{"stitch", "x-stitch-backend", "x-stitch-earliest-capability"} {
			if req.Header.Get(header) != "" {
				r.leaked.Store(true)
			}
		}
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":{"height":"127000005","fixture":"verified"}}`)
	}))
	preferred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r.preferredHits.Add(1)
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"historical data unavailable"}}`)
	}))
	t.Cleanup(hinted.Close)
	t.Cleanup(preferred.Close)
	bs := []*backend.Backend{
		{Name: "preferred-but-missing", Coverage: backend.Coverage{Kind: backend.CovBounded, Lower: 127_000_000, Upper: 130_000_000}, Weight: 1000, Endpoints: map[types.Protocol]string{types.ProtoRPC: preferred.URL}},
		{Name: "discovered", Coverage: backend.Coverage{Kind: backend.CovBounded, Lower: 127_000_000, Upper: 130_000_000}, Weight: 100, Endpoints: map[types.Protocol]string{types.ProtoRPC: hinted.URL}},
	}
	r.reg = backend.NewRegistry(bs)
	r.health = health.NewRegistry()
	for _, b := range bs {
		r.health.Update(health.Snapshot{Backend: b.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 130_000_000})
	}
	r.circuit = circuit.NewManager(circuit.Policy{ErrorThreshold: .5, MinRequests: 1, OpenDuration: time.Minute})
	hp := pool.NewHTTPPool()
	t.Cleanup(hp.CloseIdle)
	fwd := forwarder.NewHTTP(selector.NewRangeSelector(r.reg, r.health, r.circuit, 0), hp, r.circuit, forwarder.Policy{MaxAttempts: 3, PerAttemptTimeout: time.Second})
	r.server = cmt_rpc.New("127.0.0.1:0", fwd)
	r.front = r.server.Handler()
	return r
}

func TestHistoricalBackendAffinityKeepsDiscoveredShard(t *testing.T) {
	for _, uri := range []bool{false, true} {
		t.Run(map[bool]string{false: "jsonrpc", true: "uri"}[uri], func(t *testing.T) {
			r := newAffinityRig(t)
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"127000005"}}`))
			if uri {
				req = httptest.NewRequest(http.MethodGet, "/block?height=127000005", nil)
			}
			req.Header.Set("x-stitch-backend", "discovered")
			req.Header.Set("stitch", "earliest")
			req.Header.Set("x-stitch-earliest-capability", "block")
			w := httptest.NewRecorder()
			r.front.ServeHTTP(w, req)
			if w.Code != http.StatusOK || !bytes.Contains(w.Body.Bytes(), []byte(`"verified"`)) {
				t.Fatalf("discovered shard lost after concrete-height routing: status=%d body=%s", w.Code, w.Body.String())
			}
			if r.hintedHits.Load() != 1 || r.preferredHits.Load() != 0 {
				t.Errorf("affinity ignored: discovered=%d preferred=%d", r.hintedHits.Load(), r.preferredHits.Load())
			}
			if r.leaked.Load() {
				t.Fatal("Stitch routing instructions leaked to ordinary node")
			}
		})
	}
}

func TestHistoricalBackendAffinityStillRequiresEligibility(t *testing.T) {
	for _, condition := range []string{"health", "drain", "circuit", "coverage", "unknown"} {
		t.Run(condition, func(t *testing.T) {
			r := newAffinityRig(t)
			height := "127000005"
			name := "discovered"
			switch condition {
			case "health":
				r.health.Update(health.Snapshot{Backend: name, Protocol: types.ProtoRPC, Healthy: false})
			case "drain":
				r.reg.Drain(name)
			case "circuit":
				r.circuit.Record(name, types.ProtoRPC, false)
			case "coverage":
				height = "1"
			case "unknown":
				name = "does-not-exist"
			}
			req := httptest.NewRequest(http.MethodGet, "/block?height="+height, nil)
			req.Header.Set("x-stitch-backend", name)
			w := httptest.NewRecorder()
			r.front.ServeHTTP(w, req)
			if w.Code != http.StatusServiceUnavailable {
				t.Errorf("ineligible pinned backend returned status=%d body=%s", w.Code, w.Body.String())
			}
			if r.hintedHits.Load() != 0 || r.preferredHits.Load() != 0 {
				t.Fatal("ineligible affinity fell back to an unverified source")
			}
		})
	}
}

func TestHistoricalBackendAffinityRejectsInvalidHint(t *testing.T) {
	for _, values := range [][]string{{""}, {"discovered", "discovered"}, {" discovered"}, {"discovered "}, {"discovered,other"}} {
		t.Run("values="+joinHint(values), func(t *testing.T) {
			r := newAffinityRig(t)
			req := httptest.NewRequest(http.MethodGet, "/block?height=127000005", nil)
			req.Header[http.CanonicalHeaderKey("x-stitch-backend")] = values
			w := httptest.NewRecorder()
			r.front.ServeHTTP(w, req)
			if w.Code != http.StatusBadRequest {
				t.Errorf("invalid hint status=%d body=%s", w.Code, w.Body.String())
			}
			if r.hintedHits.Load()+r.preferredHits.Load() != 0 {
				t.Fatal("invalid hint reached upstream")
			}
		})
	}
	for _, path := range []string{"/block", "/status", "/broadcast_tx_sync?tx=0x00"} {
		t.Run(path, func(t *testing.T) {
			r := newAffinityRig(t)
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Header.Set("x-stitch-backend", "discovered")
			w := httptest.NewRecorder()
			r.front.ServeHTTP(w, req)
			if w.Code != http.StatusBadRequest || r.hintedHits.Load()+r.preferredHits.Load() != 0 {
				t.Errorf("nonhistorical read/write accepted affinity: status=%d", w.Code)
			}
		})
	}
}

func TestHistoricalRequestWithoutAffinityKeepsNormalSelection(t *testing.T) {
	r := newAffinityRig(t)
	req := httptest.NewRequest(http.MethodGet, "/block?height=127000005", nil)
	w := httptest.NewRecorder()
	r.front.ServeHTTP(w, req)
	if r.preferredHits.Load() != 1 || r.hintedHits.Load() != 0 {
		t.Errorf("ordinary selector preference changed: preferred=%d discovered=%d", r.preferredHits.Load(), r.hintedHits.Load())
	}
}

func TestHistoricalBackendAffinityDoesNotReuseUnverifiedCacheEntry(t *testing.T) {
	r := newAffinityRig(t)
	r.server.SetResponseCache(cache.NewResponseCache(cache.ResponseCacheOpts{Capacity: 10}), func() int64 { return 150_000_000 }, 10, time.Minute)
	// The ordinary higher-weight backend responds HTTP 200 with JSON-RPC
	// missing-history error. A discovered source must not reuse this entry.
	warm := httptest.NewRecorder()
	r.front.ServeHTTP(warm, httptest.NewRequest(http.MethodGet, "/block?height=127000005", nil))
	if r.preferredHits.Load() != 1 || !bytes.Contains(warm.Body.Bytes(), []byte("historical data unavailable")) {
		t.Fatalf("fixture cache warmup failed: %s", warm.Body.String())
	}
	request := httptest.NewRequest(http.MethodGet, "/block?height=127000005", nil)
	request.Header.Set("x-stitch-backend", "discovered")
	w := httptest.NewRecorder()
	r.front.ServeHTTP(w, request)
	if w.Code != http.StatusOK || !bytes.Contains(w.Body.Bytes(), []byte(`"verified"`)) || r.hintedHits.Load() != 1 {
		t.Fatalf("affinity reused unverified cache entry: status=%d body=%s discovered-hits=%d", w.Code, w.Body.String(), r.hintedHits.Load())
	}

	// A warm cache must not bypass the pinned source's health gate either.
	r.reg.Drain("discovered")
	again := httptest.NewRecorder()
	r.front.ServeHTTP(again, request.Clone(request.Context()))
	if again.Code != http.StatusServiceUnavailable || r.hintedHits.Load() != 1 {
		t.Fatalf("cache bypassed affinity eligibility: status=%d body=%s", again.Code, again.Body.String())
	}
}

func joinHint(values []string) string {
	var out string
	for _, value := range values {
		out += "[" + value + "]"
	}
	return out
}
