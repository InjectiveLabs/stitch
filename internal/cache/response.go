package cache

import (
	"container/list"
	"sync"
	"time"

	"github.com/decentrio/stitch/internal/metrics"
)

// ResponseCache is a TTL'd LRU storing whole JSON-RPC response bodies.
// Reads are O(1); the LRU touches are protected by a single mutex
// (good enough for stitch-scale concurrency; Phase 8+ may swap for
// ristretto if profiling demands it).
//
// Bytes are stored unmodified — caller decides whether to canonicalize
// before insertion. The cache neither parses nor mutates response
// payloads, so any JSON-RPC response shape is fine.
type ResponseCache struct {
	mu       sync.Mutex
	entries  map[string]*list.Element
	order    *list.List
	capacity int
	bytesIn  int64 // approximate; updated on every Set/evict
	maxBytes int64 // 0 = unbounded by bytes
}

type cacheEntry struct {
	key     string
	body    []byte
	expires time.Time // zero = no TTL
}

// ResponseCacheOpts tunes capacity and byte budget.
type ResponseCacheOpts struct {
	Capacity int   // max entries; 0 = unlimited (only useful for tests)
	MaxBytes int64 // approximate byte budget across all entries; 0 = unlimited
}

func NewResponseCache(opts ResponseCacheOpts) *ResponseCache {
	return &ResponseCache{
		entries:  make(map[string]*list.Element),
		order:    list.New(),
		capacity: opts.Capacity,
		maxBytes: opts.MaxBytes,
	}
}

// Get returns a copy of the cached body or (nil, false). Expired entries
// are pruned on access. On hit, the entry moves to the front of the LRU.
// Callers may mutate the returned slice freely.
//
// The copy happens OUTSIDE the critical section so a large body doesn't
// serialize every other cache user behind a memcpy. That is safe because
// stored bodies are immutable: Set always copies in, and overwriting a
// key swaps the entry's slice rather than writing through it, so a slice
// captured under the lock can never be concurrently modified.
func (c *ResponseCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	elem, ok := c.entries[key]
	if !ok {
		c.mu.Unlock()
		metrics.CacheTotal.WithLabelValues("response", "miss").Inc()
		return nil, false
	}
	e := elem.Value.(*cacheEntry)
	if !e.expires.IsZero() && time.Now().After(e.expires) {
		c.removeElement(elem)
		c.mu.Unlock()
		metrics.CacheTotal.WithLabelValues("response", "expired").Inc()
		return nil, false
	}
	c.order.MoveToFront(elem)
	body := e.body
	c.mu.Unlock()

	metrics.CacheTotal.WithLabelValues("response", "hit").Inc()
	out := make([]byte, len(body))
	copy(out, body)
	return out, true
}

// Set binds key to body with optional TTL. ttl ≤ 0 means no expiration.
//
// Bodies larger than MaxBytes (per-entry) are rejected to avoid one
// large response evicting everything else.
func (c *ResponseCache) Set(key string, body []byte, ttl time.Duration) {
	if key == "" || len(body) == 0 {
		return
	}
	if c.maxBytes > 0 && int64(len(body)) > c.maxBytes/2 {
		// One entry shouldn't dominate the cache.
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[key]; ok {
		old := elem.Value.(*cacheEntry)
		c.bytesIn -= int64(len(old.body))
		bodyCopy := make([]byte, len(body))
		copy(bodyCopy, body)
		old.body = bodyCopy
		old.expires = expiry(ttl)
		c.bytesIn += int64(len(bodyCopy))
		c.order.MoveToFront(elem)
		return
	}

	bodyCopy := make([]byte, len(body))
	copy(bodyCopy, body)
	elem := c.order.PushFront(&cacheEntry{key: key, body: bodyCopy, expires: expiry(ttl)})
	c.entries[key] = elem
	c.bytesIn += int64(len(bodyCopy))

	c.evictUntilWithinBudget()
}

// Delete drops the binding for key, if present.
func (c *ResponseCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.entries[key]; ok {
		c.removeElement(elem)
	}
}

// Purge drops every entry, resets byte accounting, and returns how many
// entries were removed.
func (c *ResponseCache) Purge() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.order.Len()
	c.entries = make(map[string]*list.Element)
	c.order.Init()
	c.bytesIn = 0
	metrics.CacheTotal.WithLabelValues("response", "purge").Add(float64(n))
	return n
}

// Size returns the current entry count.
func (c *ResponseCache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// Bytes returns the approximate cached byte total.
func (c *ResponseCache) Bytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytesIn
}

// removeElement drops elem under lock.
func (c *ResponseCache) removeElement(elem *list.Element) {
	e := elem.Value.(*cacheEntry)
	c.order.Remove(elem)
	delete(c.entries, e.key)
	c.bytesIn -= int64(len(e.body))
	metrics.CacheTotal.WithLabelValues("response", "evict").Inc()
}

func (c *ResponseCache) evictUntilWithinBudget() {
	for {
		over := false
		if c.capacity > 0 && c.order.Len() > c.capacity {
			over = true
		}
		if c.maxBytes > 0 && c.bytesIn > c.maxBytes {
			over = true
		}
		if !over {
			return
		}
		oldest := c.order.Back()
		if oldest == nil {
			return
		}
		c.removeElement(oldest)
	}
}

func expiry(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return time.Now().Add(ttl)
}
