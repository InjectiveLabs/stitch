package eth_rpc

import (
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/cache"
)

// TestRespCacheServesFromCacheOnFinalizedHeight: a cacheable, height-keyed
// read at a finalized block returns from cache on the second hit and
// the upstream is not contacted.
func TestRespCacheServesFromCacheOnFinalizedHeight(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	rc := cache.NewResponseCache(cache.ResponseCacheOpts{Capacity: 100})
	var head atomic.Int64
	head.Store(100000)
	r.front.SetResponseCache(rc, head.Load, 100, 5*time.Minute)

	// eth_getBalance at block 12345 — well below head − 100.
	body := `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xabcd","0x3039"]}`

	// First call: miss, upstream hit, populates cache.
	resp1 := post(t, r.frontT.URL, body)
	resp1Body, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	if got := resp1.Header.Get("x-stitch-cache"); got != "miss" {
		t.Errorf("first call x-stitch-cache=%q (expected miss)", got)
	}
	hitsAfterFirst := r.shard.hits.Load() + r.archive.hits.Load()

	// Second call: hit, no upstream traffic.
	resp2 := post(t, r.frontT.URL, body)
	resp2Body, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if got := resp2.Header.Get("x-stitch-cache"); got != "hit" {
		t.Errorf("second call x-stitch-cache=%q (expected hit)", got)
	}
	hitsAfterSecond := r.shard.hits.Load() + r.archive.hits.Load()
	if hitsAfterSecond != hitsAfterFirst {
		t.Errorf("upstream hit on cached call: %d new hits", hitsAfterSecond-hitsAfterFirst)
	}
	if string(resp1Body) != string(resp2Body) {
		t.Errorf("body diverged across miss/hit: %s vs %s", resp1Body, resp2Body)
	}
}

// TestRespCacheBypassedAtNonFinalizedHeight: a height too close to head
// (within confirmation depth) is not cached.
func TestRespCacheBypassedAtNonFinalizedHeight(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	rc := cache.NewResponseCache(cache.ResponseCacheOpts{Capacity: 100})
	var head atomic.Int64
	head.Store(100000)
	r.front.SetResponseCache(rc, head.Load, 100, 5*time.Minute)

	// eth_getBalance at block 99999 — within confirmation depth (100k - 100 = 99900).
	body := `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xabcd","0x1869F"]}`

	resp := post(t, r.frontT.URL, body)
	defer resp.Body.Close()
	if got := resp.Header.Get("x-stitch-cache"); got == "miss" || got == "hit" {
		t.Errorf("non-finalized height should bypass cache; got x-stitch-cache=%q", got)
	}
	if rc.Size() != 0 {
		t.Errorf("cache should not have populated; size=%d", rc.Size())
	}
}

// TestRespCacheParamSensitivity: same method+height with different params
// keys to different cache entries.
func TestRespCacheParamSensitivity(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	rc := cache.NewResponseCache(cache.ResponseCacheOpts{Capacity: 100})
	var head atomic.Int64
	head.Store(100000)
	r.front.SetResponseCache(rc, head.Load, 100, 5*time.Minute)

	body1 := `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xaaaa","0x3039"]}`
	body2 := `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xbbbb","0x3039"]}`

	post(t, r.frontT.URL, body1).Body.Close()
	post(t, r.frontT.URL, body2).Body.Close()

	if rc.Size() != 2 {
		t.Errorf("expected 2 cache entries (different addresses); got %d", rc.Size())
	}
}

// TestRespCacheNonCacheableMethodSkipped: a method without cacheable: true
// in the manifest does not populate the cache regardless of finalization.
func TestRespCacheNonCacheableMethodSkipped(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	rc := cache.NewResponseCache(cache.ResponseCacheOpts{Capacity: 100})
	var head atomic.Int64
	head.Store(100000)
	r.front.SetResponseCache(rc, head.Load, 100, 5*time.Minute)

	// eth_blockNumber is height: latest, not cacheable.
	body := `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`
	resp := post(t, r.frontT.URL, body)
	defer resp.Body.Close()

	if rc.Size() != 0 {
		t.Errorf("non-cacheable method must not populate cache; size=%d", rc.Size())
	}
	if hdr := resp.Header.Get("x-stitch-cache"); hdr != "" {
		t.Errorf("non-cacheable should not stamp x-stitch-cache; got %q", hdr)
	}
	_ = strings.Contains // silence lint
}

// TestRespCacheUsesConfiguredTTL: entries stored by the server expire after
// the TTL passed to SetResponseCache, so a later identical call misses.
func TestRespCacheUsesConfiguredTTL(t *testing.T) {
	r := setupEth(t)
	defer r.close()

	rc := cache.NewResponseCache(cache.ResponseCacheOpts{Capacity: 100})
	var head atomic.Int64
	head.Store(100000)
	r.front.SetResponseCache(rc, head.Load, 100, time.Millisecond)

	body := `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xabcd","0x3039"]}`

	post(t, r.frontT.URL, body).Body.Close() // miss → populate with 1ms TTL
	time.Sleep(50 * time.Millisecond)

	resp := post(t, r.frontT.URL, body)
	defer resp.Body.Close()
	if got := resp.Header.Get("x-stitch-cache"); got != "miss" {
		t.Errorf("entry should have expired after the configured TTL; x-stitch-cache=%q", got)
	}
}
