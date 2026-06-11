package cache

import (
	"strconv"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/decentrio/stitch/internal/metrics"
)

func TestHashIndexBasic(t *testing.T) {
	h := New(10)
	if _, ok := h.Get("absent"); ok {
		t.Error("absent key should miss")
	}
	h.Set("0xabc", 42)
	if got, ok := h.Get("0xabc"); !ok || got != 42 {
		t.Errorf("got %d %v", got, ok)
	}
	if h.Size() != 1 {
		t.Errorf("size: %d", h.Size())
	}
}

func TestHashIndexEvictionLRU(t *testing.T) {
	h := New(3)
	h.Set("a", 1)
	h.Set("b", 2)
	h.Set("c", 3)
	// a is least-recently-used; c is most.
	h.Set("d", 4) // should evict a
	if _, ok := h.Get("a"); ok {
		t.Error("a should have been evicted")
	}
	for _, k := range []string{"b", "c", "d"} {
		if _, ok := h.Get(k); !ok {
			t.Errorf("expected %s to be present", k)
		}
	}
	if h.Size() != 3 {
		t.Errorf("size: %d", h.Size())
	}
}

func TestHashIndexGetMovesToFront(t *testing.T) {
	h := New(3)
	h.Set("a", 1)
	h.Set("b", 2)
	h.Set("c", 3)
	// Touch a — it becomes most-recent.
	if _, ok := h.Get("a"); !ok {
		t.Fatal("a should be present")
	}
	// Now b is LRU.
	h.Set("d", 4) // should evict b
	if _, ok := h.Get("b"); ok {
		t.Error("b should have been evicted (a was touched)")
	}
	if _, ok := h.Get("a"); !ok {
		t.Error("a should still be present")
	}
}

func TestHashIndexOverwrite(t *testing.T) {
	h := New(10)
	h.Set("0xabc", 42)
	h.Set("0xabc", 100) // last writer wins
	if got, _ := h.Get("0xabc"); got != 100 {
		t.Errorf("got %d", got)
	}
	if h.Size() != 1 {
		t.Errorf("size after overwrite: %d", h.Size())
	}
}

func TestHashIndexRejectsZero(t *testing.T) {
	h := New(10)
	h.Set("", 42)      // empty hash
	h.Set("0xabc", 0)  // zero height
	h.Set("0xdef", -1) // negative height
	if h.Size() != 0 {
		t.Errorf("size: %d (expected 0; invalid entries should be rejected)", h.Size())
	}
}

func TestHashIndexPurge(t *testing.T) {
	h := New(10)
	for i := 0; i < 5; i++ {
		h.Set("h"+strconv.Itoa(i), int64(i+1))
	}
	purged := metrics.CacheTotal.WithLabelValues("hashidx", "purge")
	before := testutil.ToFloat64(purged)
	if n := h.Purge(); n != 5 {
		t.Errorf("purge returned %d; want 5", n)
	}
	if got := testutil.ToFloat64(purged) - before; got != 5 {
		t.Errorf("cache_total{hashidx,purge} advanced by %v; want 5", got)
	}
	if h.Size() != 0 {
		t.Errorf("size after purge: %d", h.Size())
	}
	if _, ok := h.Get("h0"); ok {
		t.Error("purged key should miss")
	}
	if n := h.Purge(); n != 0 {
		t.Errorf("second purge returned %d; want 0", n)
	}
	// Cache still works after a purge.
	h.Set("again", 9)
	if got, ok := h.Get("again"); !ok || got != 9 {
		t.Errorf("set-after-purge: got %d %v", got, ok)
	}
}

func TestHashIndexUnboundedZeroCapacity(t *testing.T) {
	h := New(0)
	for i := 0; i < 1000; i++ {
		h.Set("h"+strconv.Itoa(i), int64(i+1))
	}
	if h.Size() != 1000 {
		t.Errorf("expected unbounded size; got %d", h.Size())
	}
}

func TestHashIndexConcurrentReads(t *testing.T) {
	h := New(100)
	for i := 0; i < 50; i++ {
		h.Set("h"+strconv.Itoa(i), int64(i+1))
	}
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				h.Get("h" + strconv.Itoa(i%50))
			}
		}()
	}
	wg.Wait()
}

func TestHashIndexConcurrentReadWrite(t *testing.T) {
	h := New(50)
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(rid int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				h.Set("r"+strconv.Itoa(rid)+":"+strconv.Itoa(i), int64(i+1))
				h.Get("r" + strconv.Itoa(rid) + ":" + strconv.Itoa(i))
			}
		}(r)
	}
	wg.Wait()
}
