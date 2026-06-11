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

// evmStreamProtocols are the request protocols whose health rides the
// shared chain-head signal (see healthProtocols).
var evmStreamProtocols = []types.Protocol{
	types.ProtoEthRPC, types.ProtoEthWS, types.ProtoChainStream,
}

// mkEVMOnly builds a registry with a single backend that exposes only
// EVM/stream endpoints — no CometBFT rpc — so the RPC prober never writes
// a ProtoRPC snapshot for it. The only possible health witness is the
// ProtoEthWS snapshot written by the eth_ws head prober.
func mkEVMOnly(t *testing.T) (*backend.Registry, *health.Registry, *circuit.Manager) {
	t.Helper()
	bs := []*backend.Backend{{
		Name:     "evm",
		Coverage: backend.Coverage{Kind: backend.CovArchive},
		Weight:   100,
		Endpoints: map[types.Protocol]string{
			types.ProtoEthRPC:      "http://evm:8545",
			types.ProtoEthWS:       "ws://evm:8546",
			types.ProtoChainStream: "evm:9900",
		},
	}}
	reg := backend.NewRegistry(bs)
	h := health.NewRegistry()
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5,
		MinRequests:    4,
		OpenDuration:   100 * time.Millisecond,
	})
	return reg, h, cm
}

// TestRangeSelectorEVMOnlyGatedByEthWSSnapshot: with no rpc endpoint there
// is no ProtoRPC snapshot, so the eth_ws prober's ProtoEthWS snapshot must
// decide eligibility for all EVM/stream protocols.
func TestRangeSelectorEVMOnlyGatedByEthWSSnapshot(t *testing.T) {
	cases := []struct {
		name    string
		healthy bool
		include bool
	}{
		{"unhealthy ws snapshot excludes", false, false},
		{"healthy ws snapshot includes", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, proto := range evmStreamProtocols {
				reg, h, cm := mkEVMOnly(t)
				h.Update(health.Snapshot{
					Backend:      "evm",
					Protocol:     types.ProtoEthWS,
					Healthy:      tc.healthy,
					LatestHeight: 100000,
				})
				// maxLag > 0 on purpose: WS snapshots carry no Lag (the
				// RPC prober owns lag), so a healthy WS witness must pass
				// the lag gate with its zero Lag.
				s := NewRangeSelector(reg, h, cm, 100)
				cands := s.Candidates(types.RouteKey{Protocol: proto, Class: types.ClassLatest})
				if got := len(cands) == 1; got != tc.include {
					t.Errorf("protocol %s: included=%v, want %v", proto, got, tc.include)
				}
			}
		})
	}
}

// TestRangeSelectorEVMOnlyNoSnapshotsOptimistic: a backend no prober covers
// at all (no rpc endpoint to poll, no WS head seen yet) stays routable.
func TestRangeSelectorEVMOnlyNoSnapshotsOptimistic(t *testing.T) {
	for _, proto := range evmStreamProtocols {
		reg, h, cm := mkEVMOnly(t)
		s := NewRangeSelector(reg, h, cm, 100)
		cands := s.Candidates(types.RouteKey{Protocol: proto, Class: types.ClassLatest})
		if len(cands) != 1 {
			t.Errorf("protocol %s: expected optimistic include, got %d candidates", proto, len(cands))
		}
	}
}

// TestRangeSelectorMappedSnapshotBeatsNative: when both ProtoRPC and
// ProtoEthWS snapshots exist, the RPC prober's verdict wins — it owns the
// authoritative height/lag signal; the WS snapshot is only the fallback
// witness for backends the RPC prober cannot see.
func TestRangeSelectorMappedSnapshotBeatsNative(t *testing.T) {
	cases := []struct {
		name    string
		rpcOK   bool
		wsOK    bool
		include bool
	}{
		{"healthy rpc beats unhealthy ws", true, false, true},
		{"unhealthy rpc beats healthy ws", false, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, proto := range evmStreamProtocols {
				bs := []*backend.Backend{{
					Name:     "dual",
					Coverage: backend.Coverage{Kind: backend.CovArchive},
					Weight:   100,
					Endpoints: map[types.Protocol]string{
						types.ProtoRPC:         "http://dual:26657",
						types.ProtoEthRPC:      "http://dual:8545",
						types.ProtoEthWS:       "ws://dual:8546",
						types.ProtoChainStream: "dual:9900",
					},
				}}
				reg := backend.NewRegistry(bs)
				h := health.NewRegistry()
				h.Update(health.Snapshot{
					Backend: "dual", Protocol: types.ProtoRPC,
					Healthy: tc.rpcOK, LatestHeight: 100000,
				})
				h.Update(health.Snapshot{
					Backend: "dual", Protocol: types.ProtoEthWS,
					Healthy: tc.wsOK, LatestHeight: 100000,
				})
				cm := circuit.NewManager(circuit.Policy{
					ErrorThreshold: 0.5, MinRequests: 4, OpenDuration: 100 * time.Millisecond,
				})
				s := NewRangeSelector(reg, h, cm, 0)
				cands := s.Candidates(types.RouteKey{Protocol: proto, Class: types.ClassLatest})
				if got := len(cands) == 1; got != tc.include {
					t.Errorf("protocol %s: included=%v, want %v", proto, got, tc.include)
				}
			}
		})
	}
}

// TestRangeSelectorHeightFloor exercises the selector floor: when the
// queried height exceeds MaxHead (stale probe / brief WS-disconnect window),
// any backend whose coverage upper bound rides head (pruned / open /
// archive) must still be treated as eligible for that height. Today the
// bug is that Eligible(h, head) requires h <= head, and head = MaxHead.
func TestRangeSelectorHeightFloor(t *testing.T) {
	cases := []struct {
		name     string
		coverage backend.Coverage
	}{
		{"pruned", backend.Coverage{Kind: backend.CovPruned, Keep: 1000}},
		{"open", backend.Coverage{Kind: backend.CovOpen, Lower: 1}},
		{"archive", backend.Coverage{Kind: backend.CovArchive}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bs := []*backend.Backend{{
				Name:      tc.name,
				Coverage:  tc.coverage,
				Weight:    100,
				Endpoints: map[types.Protocol]string{types.ProtoEthRPC: "http://" + tc.name + ":8545"},
			}}
			reg := backend.NewRegistry(bs)
			h := health.NewRegistry()
			// MaxHead lags real chain by 50 blocks — the race window.
			h.Update(health.Snapshot{
				Backend:      tc.name,
				Protocol:     types.ProtoRPC,
				Healthy:      true,
				LatestHeight: 100_000,
			})
			cm := circuit.NewManager(circuit.Policy{ErrorThreshold: 0.5, MinRequests: 4, OpenDuration: 100 * time.Millisecond})
			s := NewRangeSelector(reg, h, cm, 0)

			queried := int64(100_050) // 50 blocks past MaxHead
			cands := s.Candidates(types.RouteKey{
				Protocol: types.ProtoEthRPC,
				Method:   "eth_getBalance",
				Class:    types.ClassByHeight,
				Height:   &queried,
			})
			if len(cands) != 1 || cands[0].Name != tc.name {
				t.Fatalf("expected %q candidate, got %v", tc.name, cands)
			}
		})
	}
}
