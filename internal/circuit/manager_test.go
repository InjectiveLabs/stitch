package circuit

import (
	"testing"
	"time"

	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/types"
)

func testManagerPolicy() Policy {
	return Policy{ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: time.Second}
}

func TestManagerPruneRemovesBreakersAndGauge(t *testing.T) {
	m := NewManager(testManagerPolicy())

	// Touch breakers for two backends across two protocols; Allow also
	// populates the CircuitState gauge children.
	m.Allow("cb-prune-gone", types.ProtoRPC)
	m.Allow("cb-prune-gone", types.ProtoEthRPC)
	m.Allow("cb-prune-keep", types.ProtoRPC)

	m.Prune(map[string]struct{}{"cb-prune-keep": {}})

	for _, p := range []types.Protocol{types.ProtoRPC, types.ProtoEthRPC} {
		if _, ok := m.breakers.Load(key{backend: "cb-prune-gone", protocol: p}); ok {
			t.Errorf("breaker for gone/%s should be pruned from the map", p)
		}
	}
	if _, ok := m.breakers.Load(key{backend: "cb-prune-keep", protocol: types.ProtoRPC}); !ok {
		t.Error("surviving backend's breaker should remain in the map")
	}

	if circuitGaugeHasBackend(t, "cb-prune-gone") {
		t.Error("stitch_circuit_state still carries pruned backend label")
	}
	if !circuitGaugeHasBackend(t, "cb-prune-keep") {
		t.Error("surviving backend's circuit gauge should remain")
	}
}

func TestManagerPruneEverything(t *testing.T) {
	m := NewManager(testManagerPolicy())
	m.Allow("cb-prune-all", types.ProtoRPC)

	m.Prune(map[string]struct{}{})

	if _, ok := m.breakers.Load(key{backend: "cb-prune-all", protocol: types.ProtoRPC}); ok {
		t.Error("breaker should be pruned")
	}
	// Manager keeps working after a full prune — breakers re-allocate lazily.
	if !m.Allow("cb-prune-all-next", types.ProtoRPC) {
		t.Error("fresh breaker after prune should allow")
	}
}

// circuitGaugeHasBackend reports whether any stitch_circuit_state sample
// carries backend=<name>.
func circuitGaugeHasBackend(t *testing.T, name string) bool {
	t.Helper()
	mfs, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "stitch_circuit_state" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "backend" && lp.GetValue() == name {
					return true
				}
			}
		}
	}
	return false
}
