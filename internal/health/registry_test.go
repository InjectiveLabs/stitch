package health

import (
	"testing"

	"github.com/InjectiveLabs/stitch/internal/metrics"
	"github.com/InjectiveLabs/stitch/internal/types"
)

func TestPruneRemovesInactiveBackend(t *testing.T) {
	r := NewRegistry()
	r.Update(Snapshot{Backend: "keep", Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100})
	r.Update(Snapshot{Backend: "gone", Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 250})
	r.Update(Snapshot{Backend: "gone", Protocol: types.ProtoEthRPC, Healthy: true})

	r.Prune(map[string]struct{}{"keep": {}})

	if _, ok := r.Get("gone", types.ProtoRPC); ok {
		t.Error("gone/rpc snapshot should be pruned")
	}
	if _, ok := r.Get("gone", types.ProtoEthRPC); ok {
		t.Error("gone/eth_rpc snapshot should be pruned")
	}
	if s, ok := r.Get("keep", types.ProtoRPC); !ok || s.LatestHeight != 100 {
		t.Errorf("keep snapshot should survive intact; got %+v ok=%v", s, ok)
	}
	if got := len(r.All()); got != 1 {
		t.Errorf("expected 1 snapshot after prune; got %d", got)
	}
	// MaxHead stays at the removed backend's maximum — chain head is
	// monotonic, so the old observation remains valid.
	if r.MaxHead() != 250 {
		t.Errorf("MaxHead should stay monotonic across prune; got %d", r.MaxHead())
	}
}

func TestPruneEverythingInactive(t *testing.T) {
	r := NewRegistry()
	r.Update(Snapshot{Backend: "a", Protocol: types.ProtoRPC})
	r.Update(Snapshot{Backend: "b", Protocol: types.ProtoRPC})

	r.Prune(map[string]struct{}{})

	if got := len(r.All()); got != 0 {
		t.Errorf("expected empty registry; got %d snapshots", got)
	}
	// Registry keeps working after a full prune.
	r.Update(Snapshot{Backend: "c", Protocol: types.ProtoRPC, LatestHeight: 7})
	if _, ok := r.Get("c", types.ProtoRPC); !ok {
		t.Error("update after prune should land")
	}
}

func TestPruneClearsBackendMetrics(t *testing.T) {
	r := NewRegistry()
	r.Update(Snapshot{Backend: "prune-m-gone", Protocol: types.ProtoRPC})
	r.Update(Snapshot{Backend: "prune-m-keep", Protocol: types.ProtoRPC})
	metrics.BackendHealth.WithLabelValues("prune-m-gone", "rpc").Set(1)
	metrics.BackendLagBlocks.WithLabelValues("prune-m-gone").Set(3)
	metrics.BackendLatency.WithLabelValues("prune-m-gone", "rpc").Observe(0.1)
	metrics.BackendHealth.WithLabelValues("prune-m-keep", "rpc").Set(1)

	r.Prune(map[string]struct{}{"prune-m-keep": {}})

	for _, family := range []string{"stitch_backend_health", "stitch_backend_lag_blocks", "stitch_backend_latency_seconds"} {
		if metricHasBackend(t, family, "prune-m-gone") {
			t.Errorf("%s still carries pruned backend label", family)
		}
	}
	if !metricHasBackend(t, "stitch_backend_health", "prune-m-keep") {
		t.Error("surviving backend's health gauge should remain")
	}
}

// metricHasBackend reports whether any sample in family carries
// backend=<name>.
func metricHasBackend(t *testing.T, family, name string) bool {
	t.Helper()
	mfs, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != family {
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
