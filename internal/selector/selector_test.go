package selector

import (
	"testing"
	"time"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/health"
	"github.com/decentrio/stitch/internal/types"
)

func mkHeight(h int64) *int64 { return &h }

func mkRegistry(t *testing.T) (*backend.Registry, *health.Registry, *circuit.Manager) {
	t.Helper()
	bs := []*backend.Backend{
		{
			Name:      "archive",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoRPC: "http://archive:26657"},
		},
		{
			Name:      "shard1",
			Coverage:  backend.Coverage{Kind: backend.CovBounded, Lower: 1, Upper: 50000},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoRPC: "http://shard1:26657"},
		},
		{
			Name:      "pruned",
			Coverage:  backend.Coverage{Kind: backend.CovPruned, Keep: 1000},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoRPC: "http://pruned:26657"},
		},
	}
	reg := backend.NewRegistry(bs)
	h := health.NewRegistry()
	for _, b := range bs {
		h.Update(health.Snapshot{
			Backend:      b.Name,
			Protocol:     types.ProtoRPC,
			Healthy:      true,
			LatestHeight: 100000,
		})
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    4,
		OpenDuration:   100 * time.Millisecond,
	})
	return reg, h, cm
}

func TestRangeSelectorPrefersNarrowerCoverageForOldBlock(t *testing.T) {
	reg, h, cm := mkRegistry(t)
	s := NewRangeSelector(reg, h, cm, 100)

	// Block 25000: archive + shard1 are eligible; pruned is not.
	cands := s.Candidates(types.RouteKey{
		Protocol: types.ProtoRPC,
		Class:    types.ClassByHeight,
		Height:   mkHeight(25000),
	})
	if len(cands) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(cands))
	}
	if cands[0].Name != "shard1" {
		t.Errorf("expected shard1 first, got %s", cands[0].Name)
	}
	if cands[1].Name != "archive" {
		t.Errorf("expected archive second, got %s", cands[1].Name)
	}
}

func TestRangeSelectorExcludesPrunedFloor(t *testing.T) {
	reg, h, cm := mkRegistry(t)
	s := NewRangeSelector(reg, h, cm, 0)

	// Block 50: pruned floor is head-keep+1 = 99001; below floor.
	cands := s.Candidates(types.RouteKey{
		Protocol: types.ProtoRPC,
		Class:    types.ClassByHeight,
		Height:   mkHeight(50),
	})
	for _, b := range cands {
		if b.Name == "pruned" {
			t.Fatal("pruned should be excluded for height 50")
		}
	}
}

func TestRangeSelectorExcludesUnhealthy(t *testing.T) {
	reg, h, cm := mkRegistry(t)
	s := NewRangeSelector(reg, h, cm, 0)

	h.Update(health.Snapshot{
		Backend:      "archive",
		Protocol:     types.ProtoRPC,
		Healthy:      false,
		LatestHeight: 100000,
	})

	cands := s.Candidates(types.RouteKey{
		Protocol: types.ProtoRPC,
		Class:    types.ClassByHeight,
		Height:   mkHeight(25000),
	})
	for _, b := range cands {
		if b.Name == "archive" {
			t.Fatal("unhealthy archive should be excluded")
		}
	}
}

func TestRangeSelectorExcludesOpenCircuit(t *testing.T) {
	reg, h, cm := mkRegistry(t)
	s := NewRangeSelector(reg, h, cm, 0)
	for i := 0; i < 8; i++ {
		cm.Record("shard1", types.ProtoRPC, false)
	}
	cands := s.Candidates(types.RouteKey{
		Protocol: types.ProtoRPC,
		Class:    types.ClassByHeight,
		Height:   mkHeight(25000),
	})
	for _, b := range cands {
		if b.Name == "shard1" {
			t.Fatal("open-circuit shard1 should be excluded")
		}
	}
}

func TestRangeSelectorEmptyWhenNoEndpoint(t *testing.T) {
	reg, h, cm := mkRegistry(t)
	s := NewRangeSelector(reg, h, cm, 0)
	cands := s.Candidates(types.RouteKey{
		Protocol: types.ProtoEthRPC,
		Class:    types.ClassLatest,
	})
	if len(cands) != 0 {
		t.Fatalf("expected 0, got %d (no backend has eth_rpc)", len(cands))
	}
}
