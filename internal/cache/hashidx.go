// Package cache holds caches that are useful enough to live across
// multiple servers — the hash→height index in particular is consulted by
// cmt_rpc, eth_rpc, and eventually chainstream when a hash-keyed query
// arrives.
//
// Phase 6 ships an in-memory LRU. Phase 7 will add persistence (Pebble)
// and warm-up; the API stays stable across both.
package cache

import (
	"container/list"
	"sync"

	"github.com/InjectiveLabs/stitch/internal/metrics"
)

// HashIndex is a thread-safe LRU mapping hash strings to block heights.
// The hash is treated as an opaque string — the caller chooses how to
// namespace (e.g. "eth_block:0x...", "cmt_tx:0x...") so different
// protocols share one cache without colliding.
type HashIndex struct {
	mu       sync.RWMutex
	capacity int
	entries  map[string]*list.Element
	order    *list.List
}

type entry struct {
	key    string
	height int64
}

// New returns an LRU sized for at most capacity entries. capacity ≤ 0
// disables eviction (entries grow unbounded — only useful for tests).
func New(capacity int) *HashIndex {
	return &HashIndex{
		capacity: capacity,
		entries:  make(map[string]*list.Element),
		order:    list.New(),
	}
}

// Get reads the height for a hash. On hit, the entry is moved to the
// front of the LRU.
func (h *HashIndex) Get(hash string) (int64, bool) {
	h.mu.Lock()
	elem, ok := h.entries[hash]
	if !ok {
		h.mu.Unlock()
		metrics.CacheTotal.WithLabelValues("hashidx", "miss").Inc()
		return 0, false
	}
	h.order.MoveToFront(elem)
	height := elem.Value.(*entry).height
	h.mu.Unlock()
	metrics.CacheTotal.WithLabelValues("hashidx", "hit").Inc()
	return height, true
}

// Set binds a hash to a height. Overwrites any prior binding (last
// writer wins, in case the same hash gets cached at different heights
// from different responses — which shouldn't happen on a finalized
// chain, but we don't want to panic if it does).
func (h *HashIndex) Set(hash string, height int64) {
	if hash == "" || height <= 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if elem, ok := h.entries[hash]; ok {
		elem.Value.(*entry).height = height
		h.order.MoveToFront(elem)
		return
	}

	elem := h.order.PushFront(&entry{key: hash, height: height})
	h.entries[hash] = elem
	if h.capacity > 0 && h.order.Len() > h.capacity {
		oldest := h.order.Back()
		if oldest != nil {
			h.order.Remove(oldest)
			delete(h.entries, oldest.Value.(*entry).key)
			metrics.CacheTotal.WithLabelValues("hashidx", "evict").Inc()
		}
	}
}

// Purge drops every entry and returns how many were removed.
func (h *HashIndex) Purge() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := h.order.Len()
	h.entries = make(map[string]*list.Element)
	h.order.Init()
	metrics.CacheTotal.WithLabelValues("hashidx", "purge").Add(float64(n))
	return n
}

// Size returns the current entry count.
func (h *HashIndex) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.order.Len()
}

// Capacity returns the configured maximum (0 = unlimited).
func (h *HashIndex) Capacity() int { return h.capacity }
