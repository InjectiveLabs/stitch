package circuit

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/types"
)

// Manager owns one Breaker per (backend, protocol) pair. Lazily allocated.
type Manager struct {
	policy   Policy
	breakers sync.Map // map[key]*Breaker
}

type key struct {
	backend  string
	protocol types.Protocol
}

func NewManager(p Policy) *Manager { return &Manager{policy: p} }

func (m *Manager) get(backend string, p types.Protocol) *Breaker {
	k := key{backend, p}
	if v, ok := m.breakers.Load(k); ok {
		return v.(*Breaker)
	}
	b := NewBreaker(m.policy)
	actual, _ := m.breakers.LoadOrStore(k, b)
	return actual.(*Breaker)
}

// Allow consults the breaker for (backend, protocol) read-only — safe for
// candidate filtering. Updates the gauge.
func (m *Manager) Allow(backend string, p types.Protocol) bool {
	b := m.get(backend, p)
	allowed := b.Allow()
	metrics.CircuitState.WithLabelValues(backend, string(p)).Set(float64(b.State()))
	return allowed
}

// Acquire admits one request to (backend, protocol); callers must Record
// the outcome so a claimed half-open canary slot is released. Updates the
// gauge (Acquire can move an open breaker to half-open).
func (m *Manager) Acquire(backend string, p types.Protocol) bool {
	b := m.get(backend, p)
	ok := b.Acquire()
	metrics.CircuitState.WithLabelValues(backend, string(p)).Set(float64(b.State()))
	return ok
}

// Release abandons an admission obtained via Acquire for (backend,
// protocol) without recording an outcome: it frees a claimed half-open
// canary slot and never adds a sample or transitions state. Updates the
// gauge.
func (m *Manager) Release(backend string, p types.Protocol) {
	b := m.get(backend, p)
	b.Release()
	metrics.CircuitState.WithLabelValues(backend, string(p)).Set(float64(b.State()))
}

// Record reports an outcome; updates the gauge.
func (m *Manager) Record(backend string, p types.Protocol, success bool) {
	b := m.get(backend, p)
	b.Record(success)
	metrics.CircuitState.WithLabelValues(backend, string(p)).Set(float64(b.State()))
}

// State returns the current circuit state — for the admin API.
func (m *Manager) State(backend string, p types.Protocol) State {
	return m.get(backend, p).State()
}

// Prune drops the breakers of every backend not present in the active set
// (e.g. removed by a hot reload) and deletes their CircuitState gauge
// children so dashboards don't show ghost circuits. The Manager owns both
// the breaker map and the gauge, so this cleanup lives here rather than in
// the health registry or the reload path.
func (m *Manager) Prune(active map[string]struct{}) {
	removed := map[string]struct{}{}
	m.breakers.Range(func(k, _ any) bool {
		kk := k.(key)
		if _, ok := active[kk.backend]; !ok {
			m.breakers.Delete(k)
			removed[kk.backend] = struct{}{}
		}
		return true
	})
	for name := range removed {
		metrics.CircuitState.DeletePartialMatch(prometheus.Labels{"backend": name})
	}
}
