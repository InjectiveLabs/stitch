package backend

import (
	"sync"
	"sync/atomic"
)

// Registry publishes the current Backend slice via an atomic pointer. The
// request path reads via Snapshot() with no locks; reload swaps via Set().
//
// Drain state is tracked separately: the operator can drain a backend
// (selector skips it; in-flight requests complete) without mutating the
// backend object. Drain state survives Set() — operators don't expect a
// reload to silently un-drain backends they were taking out of rotation.
type Registry struct {
	current atomic.Pointer[[]*Backend]

	mu      sync.RWMutex
	drained map[string]struct{}
}

func NewRegistry(initial []*Backend) *Registry {
	r := &Registry{drained: make(map[string]struct{})}
	r.Set(initial)
	return r
}

// Set publishes a new backend slice. The previous slice remains valid for
// any in-flight callers that already loaded it.
func (r *Registry) Set(bs []*Backend) {
	cp := make([]*Backend, len(bs))
	copy(cp, bs)
	r.current.Store(&cp)
}

// Snapshot returns the current backend slice. Callers must treat it as
// read-only.
func (r *Registry) Snapshot() []*Backend {
	p := r.current.Load()
	if p == nil {
		return nil
	}
	return *p
}

// Find returns the backend with the given name from the current snapshot,
// or nil.
func (r *Registry) Find(name string) *Backend {
	for _, b := range r.Snapshot() {
		if b.Name == name {
			return b
		}
	}
	return nil
}

// Drain marks a backend as drained — selector skips it. No-op if the
// backend doesn't exist in the current snapshot (we still record the
// drain so a future reload that re-adds the name picks it up drained).
func (r *Registry) Drain(name string) {
	if name == "" {
		return
	}
	r.mu.Lock()
	r.drained[name] = struct{}{}
	r.mu.Unlock()
}

// Enable clears drain state.
func (r *Registry) Enable(name string) {
	r.mu.Lock()
	delete(r.drained, name)
	r.mu.Unlock()
}

// IsDrained reports whether the backend is drained.
func (r *Registry) IsDrained(name string) bool {
	r.mu.RLock()
	_, ok := r.drained[name]
	r.mu.RUnlock()
	return ok
}

// DrainedNames returns the current set of drained backend names — used
// by the admin API for reporting.
func (r *Registry) DrainedNames() []string {
	r.mu.RLock()
	out := make([]string, 0, len(r.drained))
	for n := range r.drained {
		out = append(out, n)
	}
	r.mu.RUnlock()
	return out
}
