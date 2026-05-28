package circuit

import (
	"sync"

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

// Allow consults the breaker for (backend, protocol) and updates the gauge.
func (m *Manager) Allow(backend string, p types.Protocol) bool {
	b := m.get(backend, p)
	allowed := b.Allow()
	metrics.CircuitState.WithLabelValues(backend, string(p)).Set(float64(b.State()))
	return allowed
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
