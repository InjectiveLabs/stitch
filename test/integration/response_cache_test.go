package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
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
	"github.com/InjectiveLabs/stitch/internal/server/eth_rpc"
	"github.com/InjectiveLabs/stitch/internal/types"
)

type responseCacheRig struct {
	handler http.Handler
	cache   *cache.ResponseCache
	hits    atomic.Int64
	reply   atomic.Value // Optional raw upstream response for admission tests.
}

// Exercise the public listeners with real HTTP forwarding to an upstream that
// echoes IDs losslessly. Payloads include nested IDs and integers beyond float64.
func newResponseCacheRig(t testing.TB, protocol types.Protocol, payload string) *responseCacheRig {
	t.Helper()
	rig := &responseCacheRig{cache: cache.NewResponseCache(cache.ResponseCacheOpts{
		Capacity: 64, MaxBytes: 4 << 20,
	})}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rig.hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if override := rig.reply.Load(); override != nil {
			_, _ = io.WriteString(w, override.(string))
			return
		}
		if r.URL.Path != "/" {
			query, _ := json.Marshal(r.URL.Query())
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":-1,"result":{"query":%s}}`, query)
			return
		}
		body, _ := io.ReadAll(r.Body)
		reply := func(raw []byte) []byte {
			var req struct{ ID json.RawMessage }
			if json.Unmarshal(raw, &req) != nil || len(req.ID) == 0 {
				return nil
			}
			return []byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":` + payload + `}`)
		}
		if bytes.HasPrefix(bytes.TrimSpace(body), []byte("[")) {
			var batch []json.RawMessage
			_ = json.Unmarshal(body, &batch)
			results := make([]json.RawMessage, 0, len(batch))
			for _, raw := range batch {
				if out := reply(raw); len(out) != 0 {
					results = append(results, out)
				}
			}
			_ = json.NewEncoder(w).Encode(results)
			return
		}
		if out := reply(body); len(out) != 0 {
			_, _ = w.Write(out)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(upstream.Close)
	backends := []*backend.Backend{{
		Name: "cache-upstream", Weight: 100, Coverage: backend.Coverage{Kind: backend.CovArchive},
		Endpoints: map[types.Protocol]string{protocol: upstream.URL},
	}}
	h := health.NewRegistry()
	h.Update(health.Snapshot{Backend: "cache-upstream", Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000})
	h.Update(health.Snapshot{Backend: "cache-upstream", Protocol: protocol, Healthy: true, LatestHeight: 100000})
	cm := circuit.NewManager(circuit.Policy{ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: time.Second})
	p := pool.NewHTTPPool()
	t.Cleanup(p.CloseIdle)
	fwd := forwarder.NewHTTP(selector.NewRangeSelector(backend.NewRegistry(backends), h, cm, 0), p, cm,
		forwarder.Policy{MaxAttempts: 1, PerAttemptTimeout: 5 * time.Second})
	if protocol == types.ProtoEthRPC {
		server := eth_rpc.New("ignored", fwd)
		server.SetResponseCache(rig.cache, func() int64 { return 100000 }, 100, 5*time.Minute)
		rig.handler = server.Handler()
	} else {
		server := cmt_rpc.New("ignored", fwd)
		server.SetResponseCache(rig.cache, func() int64 { return 100000 }, 100, 5*time.Minute)
		rig.handler = server.Handler()
	}
	return rig
}

func (r *responseCacheRig) request(method, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	out := httptest.NewRecorder()
	r.handler.ServeHTTP(out, req)
	return out
}

func cacheRPCRequest(method, params, id string) string {
	if id == "" {
		return fmt.Sprintf(`{"jsonrpc":"2.0","method":%q,"params":%s}`, method, params)
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","method":%q,"params":%s,"id":%s}`, method, params, id)
}

func assertResponseCacheID(t testing.TB, body []byte, want string) json.RawMessage {
	t.Helper()
	var response struct {
		ID     json.RawMessage `json:"id"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("invalid response: %s: %v", body, err)
	}
	if string(response.ID) != want {
		t.Errorf("response ID = %s; want %s", response.ID, want)
	}
	return response.Result
}

func TestResponseCacheCanonicalRequestsAndCallerIDs(t *testing.T) {
	const payload = `{"id":"nested-id", "large":900719925474099312345, "value":"unchanged"}`
	for _, tc := range []struct {
		protocol types.Protocol
		method   string
		params   [2]string
	}{
		{types.ProtoEthRPC, "eth_getBalance", [2]string{
			`["0xabcd",{"blockNumber":"0x3039","requireCanonical":true}]`,
			`[ "0xabcd", { "requireCanonical" : true, "blockNumber" : "0x3039" } ]`,
		}},
		{types.ProtoRPC, "validators", [2]string{
			`{"height":"12345","page":"1","per_page":"30"}`,
			`{ "per_page" : "30", "page" : "1", "height" : "12345" }`,
		}},
	} {
		t.Run(string(tc.protocol), func(t *testing.T) {
			rig := newResponseCacheRig(t, tc.protocol, payload)
			ids := []string{`1`, `2`, `900719925474099312345`, `"client-\"quoted\"-id"`, `null`, `-42`}
			for i, id := range ids {
				out := rig.request(http.MethodPost, "/", cacheRPCRequest(tc.method, tc.params[i%2], id))
				wantCache := "hit"
				if i == 0 {
					wantCache = "miss"
				}
				if got := out.Header().Get("x-stitch-cache"); got != wantCache {
					t.Errorf("call %d cache = %q; want %q", i, got, wantCache)
				}
				if result := assertResponseCacheID(t, out.Body.Bytes(), id); string(result) != payload {
					t.Errorf("response payload changed: %s", result)
				}
			}
			if rig.cache.Size() != 1 || rig.hits.Load() != 1 {
				t.Errorf("equivalent requests: entries=%d upstream calls=%d; want 1 each", rig.cache.Size(), rig.hits.Load())
			}
		})
	}
}

func TestResponseCacheDistinctParams(t *testing.T) {
	for _, tc := range []struct {
		protocol types.Protocol
		method   string
		params   []string
	}{
		{types.ProtoEthRPC, "eth_getStorageAt", []string{
			`["0xabcd",9007199254740992,"0x3039"]`,
			`["0xabcd",9007199254740993,"0x3039"]`,
			`["0xdef0",9007199254740993,"0x3039"]`,
		}},
		{types.ProtoRPC, "validators", []string{
			`{"height":"12345","page":1,"per_page":30}`,
			`{"height":"12345","page":2,"per_page":30}`,
			`{"height":"12345","page":2,"per_page":50}`,
		}},
	} {
		t.Run(string(tc.protocol), func(t *testing.T) {
			rig := newResponseCacheRig(t, tc.protocol, `"ok"`)
			for _, params := range tc.params {
				out := rig.request(http.MethodPost, "/", cacheRPCRequest(tc.method, params, `1`))
				if got := out.Header().Get("x-stitch-cache"); got != "miss" {
					t.Errorf("different params %s: cache=%q; want miss", params, got)
				}
			}
			if got := rig.cache.Size(); got != len(tc.params) {
				t.Errorf("entries=%d; want %d", got, len(tc.params))
			}
		})
	}
}

func TestResponseCacheCometURIQueryIdentity(t *testing.T) {
	rig := newResponseCacheRig(t, types.ProtoRPC, `"ok"`)
	for _, tc := range []struct {
		target, wantCache string
	}{
		{"/validators?height=12345&page=1&per_page=30", "miss"},
		{"/validators?per_page=30&page=1&height=12345", "hit"},
		{"/validators?height=12345&page=2&per_page=30", "miss"},
		{"/validators?height=12345&page=2&per_page=50", "miss"},
		{"/validators?height=12345&page=1&page=2&per_page=30", "miss"},
		{"/validators?height=12345&page=2&page=1&per_page=30", "miss"},
	} {
		out := rig.request(http.MethodGet, tc.target, "")
		if got := out.Header().Get("x-stitch-cache"); got != tc.wantCache {
			t.Errorf("%s: cache=%q; want %q", tc.target, got, tc.wantCache)
		}
		result := assertResponseCacheID(t, out.Body.Bytes(), `-1`)
		var got struct{ Query map[string][]string }
		if err := json.Unmarshal(result, &got); err != nil {
			t.Fatal(err)
		}
		want, _ := json.Marshal(httptest.NewRequest(http.MethodGet, tc.target, nil).URL.Query())
		actual, _ := json.Marshal(got.Query)
		if !bytes.Equal(actual, want) {
			t.Errorf("%s: wrong cached query: %s; want %s", tc.target, actual, want)
		}
	}
	if rig.cache.Size() != 5 || rig.hits.Load() != 5 {
		t.Errorf("URI entries=%d upstream calls=%d; want 5 each", rig.cache.Size(), rig.hits.Load())
	}
	// URI envelopes use the upstream ID, so must never reuse a JSON-RPC
	// envelope whose ID belongs to an individual caller.
	out := rig.request(http.MethodPost, "/", cacheRPCRequest("validators", `{"height":"12345","page":"1","per_page":"30"}`, `"rpc-client"`))
	if got := out.Header().Get("x-stitch-cache"); got != "miss" {
		t.Errorf("JSON-RPC collided with URI request: cache=%q", got)
	}
	assertResponseCacheID(t, out.Body.Bytes(), `"rpc-client"`)
}

func TestResponseCacheNotificationsBypass(t *testing.T) {
	for _, protocol := range []types.Protocol{types.ProtoEthRPC, types.ProtoRPC} {
		t.Run(string(protocol), func(t *testing.T) {
			method, params := "eth_getBalance", `["0xabcd","0x3039"]`
			if protocol == types.ProtoRPC {
				method, params = "block", `{"height":"12345"}`
			}
			rig := newResponseCacheRig(t, protocol, `"ok"`)
			rig.request(http.MethodPost, "/", cacheRPCRequest(method, params, `1`))
			for i := 0; i < 2; i++ {
				out := rig.request(http.MethodPost, "/", cacheRPCRequest(method, params, ""))
				if got := out.Header().Get("x-stitch-cache"); got != "" {
					t.Errorf("notification entered response cache: %q", got)
				}
				if out.Body.Len() != 0 {
					t.Errorf("notification returned a cached reply: %s", out.Body.Bytes())
				}
			}
			if rig.cache.Size() != 1 || rig.hits.Load() != 3 {
				t.Errorf("notification entries=%d upstream calls=%d; want 1 and 3", rig.cache.Size(), rig.hits.Load())
			}
		})
	}
}

func TestResponseCacheInvalidRequestsAndResponsesBypass(t *testing.T) {
	for _, protocol := range []types.Protocol{types.ProtoEthRPC, types.ProtoRPC} {
		t.Run(string(protocol), func(t *testing.T) {
			method, params := "eth_getBalance", `["0xabcd","0x3039"]`
			if protocol == types.ProtoRPC {
				method, params = "block", `{"height":"12345"}`
			}
			t.Run("request envelopes", func(t *testing.T) {
				rig := newResponseCacheRig(t, protocol, `"ok"`)
				valid := cacheRPCRequest(method, params, `1`)
				for _, request := range []string{
					cacheRPCRequest(method, params, `true`),
					cacheRPCRequest(method, params, `{}`),
					cacheRPCRequest(method, params, `[]`),
					strings.Replace(valid, `"jsonrpc":"2.0",`, "", 1),
					strings.Replace(valid, `"jsonrpc":"2.0"`, `"jsonrpc":"1.0"`, 1),
				} {
					out := rig.request(http.MethodPost, "/", request)
					if got := out.Header().Get("x-stitch-cache"); got != "" {
						t.Errorf("invalid envelope used cache: %s; cache=%q", request, got)
					}
				}
				if got := rig.cache.Size(); got != 0 {
					t.Errorf("invalid envelopes populated %d entries", got)
				}
			})
			t.Run("HTTP 200 invalid responses", func(t *testing.T) {
				rig := newResponseCacheRig(t, protocol, `"ok"`)
				for _, body := range []string{
					`not json`,
					`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"temporarily unavailable"}}`,
					`{"jsonrpc":"2.0","result":"missing id"}`,
					`{"jsonrpc":"2.0","id":1,"id":2,"result":"ambiguous"}`,
					`{"jsonrpc":"1.0","id":1,"result":"wrong version"}`,
				} {
					rig.reply.Store(body)
					for i := 0; i < 2; i++ {
						out := rig.request(http.MethodPost, "/", cacheRPCRequest(method, params, strconv.Itoa(i)))
						if out.Body.String() != body {
							t.Errorf("upstream response changed: got %s; want %s", out.Body.Bytes(), body)
						}
						if got := out.Header().Get("x-stitch-cache"); got != "miss" {
							t.Errorf("invalid response reused: cache=%q body=%s", got, body)
						}
					}
					if got := rig.cache.Size(); got != 0 {
						t.Errorf("invalid upstream response populated %d entries: %s", got, body)
					}
				}
				if got := rig.hits.Load(); got != 10 {
					t.Errorf("invalid responses upstream calls=%d; want 10", got)
				}
			})
		})
	}
}

func TestResponseCacheCometAmbiguousURIBypass(t *testing.T) {
	rig := newResponseCacheRig(t, types.ProtoRPC, `"ok"`)
	for _, tc := range []struct{ method, target, body string }{
		{http.MethodPost, "/validators?height=12345", "page=1"},
		{http.MethodPost, "/validators?height=12345", "page=2"},
		{http.MethodGet, "/validators?height=12345&page=%zz", ""},
	} {
		out := rig.request(tc.method, tc.target, tc.body)
		if got := out.Header().Get("x-stitch-cache"); got != "" {
			t.Errorf("ambiguous URI request entered cache: %s %s %q: cache=%q", tc.method, tc.target, tc.body, got)
		}
	}
	if rig.cache.Size() != 0 || rig.hits.Load() != 3 {
		t.Errorf("ambiguous URI entries=%d upstream calls=%d; want 0 and 3", rig.cache.Size(), rig.hits.Load())
	}
}

func TestResponseCacheBatchCallerIDs(t *testing.T) {
	for _, protocol := range []types.Protocol{types.ProtoEthRPC, types.ProtoRPC} {
		t.Run(string(protocol), func(t *testing.T) {
			method, params := "eth_getBalance", `["0xabcd","0x3039"]`
			if protocol == types.ProtoRPC {
				method, params = "block", `{"height":"12345"}`
			}
			rig := newResponseCacheRig(t, protocol, `{"id":"nested","n":9007199254740993}`)
			for round := 0; round < 2; round++ {
				ids := []string{strconv.Itoa(round + 1), `900719925474099312345`, `"batch-client"`, `null`}
				requests := make([]string, len(ids))
				for i, id := range ids {
					requests[i] = cacheRPCRequest(method, params, id)
				}
				out := rig.request(http.MethodPost, "/", "["+strings.Join(requests, ",")+"]")
				var responses []json.RawMessage
				if err := json.Unmarshal(out.Body.Bytes(), &responses); err != nil {
					t.Fatalf("batch returned invalid JSON: %s: %v", out.Body.Bytes(), err)
				}
				if len(responses) != len(ids) {
					t.Fatalf("batch responses=%d; want %d", len(responses), len(ids))
				}
				for i, response := range responses {
					assertResponseCacheID(t, response, ids[i])
				}
			}
			if protocol == types.ProtoEthRPC {
				if rig.cache.Size() != 1 || rig.hits.Load() != 1 {
					t.Errorf("EVM batch entries=%d upstream calls=%d; want 1 each", rig.cache.Size(), rig.hits.Load())
				}
			} else if rig.cache.Size() != 0 || rig.hits.Load() != 2 {
				t.Errorf("Comet batches must pass through: entries=%d upstream calls=%d", rig.cache.Size(), rig.hits.Load())
			}
		})
	}
}

func TestResponseCacheConcurrentLargeResponses(t *testing.T) {
	const payloadBytes = 128 << 10
	for _, protocol := range []types.Protocol{types.ProtoEthRPC, types.ProtoRPC} {
		t.Run(string(protocol), func(t *testing.T) {
			method, params := "eth_getBalance", `["0xabcd","0x3039"]`
			if protocol == types.ProtoRPC {
				method, params = "block", `{"height":"12345"}`
			}
			rig := newResponseCacheRig(t, protocol, `"`+strings.Repeat("x", payloadBytes)+`"`)
			rig.request(http.MethodPost, "/", cacheRPCRequest(method, params, `0`))
			retained := rig.cache.Bytes()
			const workers, calls, phases = 8, 32, 3
			runtime.GC()
			var baseline runtime.MemStats
			runtime.ReadMemStats(&baseline)
			for phase := 0; phase < phases; phase++ {
				var wg sync.WaitGroup
				for worker := 0; worker < workers; worker++ {
					wg.Add(1)
					go func(worker int) {
						defer wg.Done()
						for i := 0; i < calls; i++ {
							id := strconv.Itoa(phase*workers*calls + worker*calls + i + 1)
							out := rig.request(http.MethodPost, "/", cacheRPCRequest(method, params, id))
							if got := out.Header().Get("x-stitch-cache"); got != "hit" {
								t.Errorf("warmed request %s: cache=%q; want hit", id, got)
								return
							}
							if result := assertResponseCacheID(t, out.Body.Bytes(), id); len(result) != payloadBytes+2 {
								t.Errorf("request %s: payload length=%d; want %d", id, len(result), payloadBytes+2)
							}
						}
					}(worker)
				}
				wg.Wait()
				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				// Leave ample slack for runtime/HTTP pools, while still catching
				// retained per-request 128 KiB copies (32 MiB in each phase).
				if after.HeapAlloc > baseline.HeapAlloc+8<<20 {
					t.Errorf("phase %d retained heap grew from %d to %d bytes", phase, baseline.HeapAlloc, after.HeapAlloc)
				}
				t.Logf("phase %d: post-GC HeapAlloc=%d baseline=%d", phase+1, after.HeapAlloc, baseline.HeapAlloc)
			}
			if rig.cache.Size() != 1 || rig.cache.Bytes() != retained || rig.hits.Load() != 1 {
				t.Errorf("unique-ID load retained entries=%d bytes=%d upstream=%d; want 1, %d, 1", rig.cache.Size(), rig.cache.Bytes(), rig.hits.Load(), retained)
			}
			t.Logf("%d requests, %d-byte responses: warmed hit rate 100%%; retained entries=%d bytes=%d", workers*calls*phases, payloadBytes, rig.cache.Size(), rig.cache.Bytes())
			runtime.KeepAlive(rig)
		})
	}
}
