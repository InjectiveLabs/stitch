package eth_rpc

import (
	"sync"
	"time"
)

// FilterStore tracks "sticky" routing for eth filter IDs. eth_newFilter
// returns an ID that's local to the backend that minted it, so subsequent
// eth_getFilterChanges / eth_getFilterLogs / eth_uninstallFilter must
// route to the same backend.
//
// The store has a TTL because backends time filters out (5 min default
// per the Injective reference §3); after that, the mapping is stale and
// the next caller will get a fresh assignment from a healthy backend.
type FilterStore struct {
	mu      sync.RWMutex
	entries map[string]filterEntry
	ttl     time.Duration
}

type filterEntry struct {
	backend string
	expires time.Time
}

func NewFilterStore(ttl time.Duration) *FilterStore {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &FilterStore{
		entries: make(map[string]filterEntry),
		ttl:     ttl,
	}
}

// Bind records that filterID was minted by backend.
func (s *FilterStore) Bind(filterID, backend string) {
	if filterID == "" || backend == "" {
		return
	}
	s.mu.Lock()
	s.entries[filterID] = filterEntry{
		backend: backend,
		expires: time.Now().Add(s.ttl),
	}
	s.mu.Unlock()
}

// Lookup returns the bound backend for filterID, or "" if absent / expired.
func (s *FilterStore) Lookup(filterID string) string {
	s.mu.RLock()
	e, ok := s.entries[filterID]
	s.mu.RUnlock()
	if !ok || time.Now().After(e.expires) {
		return ""
	}
	return e.backend
}

// Forget drops the binding (called on uninstall).
func (s *FilterStore) Forget(filterID string) {
	s.mu.Lock()
	delete(s.entries, filterID)
	s.mu.Unlock()
}

// Sweep prunes expired entries. Safe to call from a ticker.
func (s *FilterStore) Sweep() {
	now := time.Now()
	s.mu.Lock()
	for id, e := range s.entries {
		if now.After(e.expires) {
			delete(s.entries, id)
		}
	}
	s.mu.Unlock()
}

// Size reports the current number of bindings — exposed for the admin API
// and tests.
func (s *FilterStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}
