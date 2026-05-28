// Package health holds per-backend health snapshots and the goroutines that
// produce them.
package health

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/decentrio/stitch/internal/types"
)

// Snapshot is a point-in-time read of one backend's health for one protocol.
// Cheap to copy by value.
type Snapshot struct {
	Backend      string
	Protocol     types.Protocol
	Healthy      bool
	LatestHeight int64
	LatencyP50   time.Duration
	ErrorRate    float64
	Lag          int64
	UpdatedAt    time.Time
	LastError    string
}

type key struct {
	backend  string
	protocol types.Protocol
}

// Registry is the health snapshot store. Reads are lock-free via atomic
// load on a sharded map; writes are infrequent (every probe interval).
type Registry struct {
	mu        sync.Mutex
	snapshots atomic.Pointer[map[key]*Snapshot]
	maxHead   atomic.Int64
}

func NewRegistry() *Registry {
	r := &Registry{}
	empty := map[key]*Snapshot{}
	r.snapshots.Store(&empty)
	return r
}

// Update publishes a new snapshot; copy-on-write so readers see consistent
// maps.
func (r *Registry) Update(s Snapshot) {
	if s.UpdatedAt.IsZero() {
		s.UpdatedAt = time.Now()
	}
	r.mu.Lock()
	old := *r.snapshots.Load()
	cp := make(map[key]*Snapshot, len(old)+1)
	for k, v := range old {
		cp[k] = v
	}
	k := key{backend: s.Backend, protocol: s.Protocol}
	v := s
	cp[k] = &v
	r.snapshots.Store(&cp)
	r.mu.Unlock()

	if s.LatestHeight > r.maxHead.Load() {
		r.maxHead.Store(s.LatestHeight)
	}
}

// Get returns the snapshot for (backend, protocol) or zero+false.
func (r *Registry) Get(backend string, p types.Protocol) (Snapshot, bool) {
	m := *r.snapshots.Load()
	v, ok := m[key{backend: backend, protocol: p}]
	if !ok {
		return Snapshot{}, false
	}
	return *v, true
}

// MaxHead returns the highest known head across all backends. Used as a
// reference for pruned-coverage eligibility and lag computation.
func (r *Registry) MaxHead() int64 { return r.maxHead.Load() }

// All returns a copy of every snapshot.
func (r *Registry) All() []Snapshot {
	m := *r.snapshots.Load()
	out := make([]Snapshot, 0, len(m))
	for _, v := range m {
		out = append(out, *v)
	}
	return out
}
