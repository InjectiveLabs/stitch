package cache

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestResponseCacheBasic(t *testing.T) {
	c := NewResponseCache(ResponseCacheOpts{Capacity: 4})
	if _, ok := c.Get("absent"); ok {
		t.Error("absent key should miss")
	}
	c.Set("k", []byte("v"), 0)
	got, ok := c.Get("k")
	if !ok || string(got) != "v" {
		t.Errorf("got %s %v", got, ok)
	}
	if c.Size() != 1 {
		t.Errorf("size: %d", c.Size())
	}
}

func TestResponseCacheTTLExpiry(t *testing.T) {
	c := NewResponseCache(ResponseCacheOpts{Capacity: 4})
	c.Set("k", []byte("v"), 20*time.Millisecond)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("immediate get should hit")
	}
	time.Sleep(40 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Error("expired entry should miss")
	}
	if c.Size() != 0 {
		t.Errorf("expired entry should be pruned; size=%d", c.Size())
	}
}

func TestResponseCacheCapacityEviction(t *testing.T) {
	c := NewResponseCache(ResponseCacheOpts{Capacity: 3})
	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	c.Set("c", []byte("3"), 0)
	c.Set("d", []byte("4"), 0) // a should be evicted
	if _, ok := c.Get("a"); ok {
		t.Error("a should have been evicted")
	}
	if c.Size() != 3 {
		t.Errorf("size: %d", c.Size())
	}
}

func TestResponseCacheGetMovesToFront(t *testing.T) {
	c := NewResponseCache(ResponseCacheOpts{Capacity: 3})
	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	c.Set("c", []byte("3"), 0)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should be present")
	}
	c.Set("d", []byte("4"), 0)
	if _, ok := c.Get("b"); ok {
		t.Error("b should have been evicted (a was touched)")
	}
}

func TestResponseCacheReturnedBodyIsCopy(t *testing.T) {
	c := NewResponseCache(ResponseCacheOpts{Capacity: 4})
	c.Set("k", []byte("hello"), 0)
	got, _ := c.Get("k")
	got[0] = 'H' // mutate caller's copy
	got2, _ := c.Get("k")
	if string(got2) != "hello" {
		t.Errorf("cached body was mutated: %s", got2)
	}
}

func TestResponseCacheByteBudgetEvicts(t *testing.T) {
	c := NewResponseCache(ResponseCacheOpts{Capacity: 1000, MaxBytes: 100})
	for i := 0; i < 50; i++ {
		c.Set("k"+strconv.Itoa(i), make([]byte, 10), 0)
	}
	if c.Bytes() > 100 {
		t.Errorf("byte budget exceeded: %d", c.Bytes())
	}
}

func TestResponseCacheRejectsOversizedEntry(t *testing.T) {
	c := NewResponseCache(ResponseCacheOpts{Capacity: 100, MaxBytes: 100})
	c.Set("huge", make([]byte, 200), 0) // over half budget → rejected
	if c.Size() != 0 {
		t.Errorf("oversized entry should be rejected; size=%d", c.Size())
	}
}

func TestResponseCachePurge(t *testing.T) {
	c := NewResponseCache(ResponseCacheOpts{Capacity: 10})
	for i := 0; i < 4; i++ {
		c.Set("k"+strconv.Itoa(i), []byte("value"), 0)
	}
	if c.Bytes() == 0 {
		t.Fatal("expected non-zero byte accounting before purge")
	}
	if n := c.Purge(); n != 4 {
		t.Errorf("purge returned %d; want 4", n)
	}
	if c.Size() != 0 {
		t.Errorf("size after purge: %d", c.Size())
	}
	if c.Bytes() != 0 {
		t.Errorf("bytes after purge: %d", c.Bytes())
	}
	if _, ok := c.Get("k0"); ok {
		t.Error("purged key should miss")
	}
	if n := c.Purge(); n != 0 {
		t.Errorf("second purge returned %d; want 0", n)
	}
	// Cache still works after a purge.
	c.Set("again", []byte("v"), 0)
	if got, ok := c.Get("again"); !ok || string(got) != "v" {
		t.Errorf("set-after-purge: got %s %v", got, ok)
	}
}

func TestResponseCacheConcurrent(t *testing.T) {
	c := NewResponseCache(ResponseCacheOpts{Capacity: 100})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(rid int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k := "r" + strconv.Itoa(rid) + ":" + strconv.Itoa(i)
				c.Set(k, []byte("v"), 0)
				c.Get(k)
			}
		}(r)
	}
	wg.Wait()
}

func TestBuildKeyShape(t *testing.T) {
	got := BuildKey("eth_rpc", "eth_getBalance", 12345, 0xdeadbeef)
	want := "eth_rpc:eth_getBalance:12345:deadbeef"
	if got != want {
		t.Errorf("got %s want %s", got, want)
	}
}

func TestHashParamsStable(t *testing.T) {
	a := HashParams([]byte(`["0xabc","0x10"]`))
	b := HashParams([]byte(`["0xabc","0x10"]`))
	c := HashParams([]byte(`["0xdef","0x10"]`))
	if a != b {
		t.Error("same bytes → same hash")
	}
	if a == c {
		t.Error("different bytes → different hash")
	}
}

func TestIsCacheableHeight(t *testing.T) {
	for _, c := range []struct {
		height, head, depth int64
		want                bool
	}{
		{100, 200, 50, true},  // 100 ≤ 150
		{151, 200, 50, false}, // 151 > 150
		{0, 200, 50, false},   // height 0 = not cacheable
		{100, 0, 50, false},   // head 0 = no signal
		{100, 100, 0, true},   // depth 0 + height = head
		{200, 200, 0, true},   // height = head, depth 0
		{200, 200, 1, false},  // height = head, depth 1 → 200 > 199
	} {
		got := IsCacheableHeight(c.height, c.head, c.depth)
		if got != c.want {
			t.Errorf("IsCacheableHeight(h=%d head=%d depth=%d) = %v; want %v", c.height, c.head, c.depth, got, c.want)
		}
	}
}

func TestIsImmutableMethod(t *testing.T) {
	yes := []string{"tx", "block_by_hash", "header_by_hash", "eth_getTransactionByHash", "eth_getTransactionReceipt", "eth_getBlockByHash"}
	for _, m := range yes {
		if !IsImmutableMethod(m) {
			t.Errorf("%s should be immutable", m)
		}
	}
	no := []string{"eth_blockNumber", "eth_chainId", "block", "eth_getBlockByNumber"}
	for _, m := range no {
		if IsImmutableMethod(m) {
			t.Errorf("%s should NOT be immutable", m)
		}
	}
}

func TestAtomicHead(t *testing.T) {
	var h AtomicHead
	if h.Get() != 0 {
		t.Error("zero value should be 0")
	}
	h.Set(42)
	if h.Get() != 42 {
		t.Error("set/get failed")
	}
}
