// Package selector picks an ordered list of backend candidates for a given
// route. The default RangeSelector hard-filters on health and circuit
// state, then scores by specificity and lag.
package selector

import (
	"sort"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/health"
	"github.com/decentrio/stitch/internal/types"
)

// Selector returns ordered candidates for a route key. Length 0 means no
// backend can serve the request.
type Selector interface {
	Candidates(types.RouteKey) []*backend.Backend
}

// Weights tune the scoring function. Defaults bias toward specificity so
// archive nodes are spared cheap historical reads. Health and circuit
// state are hard gates in Candidates, not score terms.
type Weights struct {
	Specificity float64
	Lag         float64
}

func DefaultWeights() Weights {
	return Weights{
		Specificity: 1.0,
		Lag:         0.2,
	}
}

// RangeSelector implements Selector based on backend coverage + health.
type RangeSelector struct {
	registry *backend.Registry
	health   *health.Registry
	circuit  *circuit.Manager
	weights  Weights
	maxLag   int64
}

func NewRangeSelector(reg *backend.Registry, h *health.Registry, c *circuit.Manager, maxLag int64) *RangeSelector {
	return &RangeSelector{
		registry: reg,
		health:   h,
		circuit:  c,
		weights:  DefaultWeights(),
		maxLag:   maxLag,
	}
}

// Candidates returns eligible backends ranked best-first.
func (s *RangeSelector) Candidates(k types.RouteKey) []*backend.Backend {
	all := s.registry.Snapshot()
	head := s.health.MaxHead()
	// Use the queried height as a floor: if the client just asked about
	// block N, treat the effective head as at least N. Covers the race
	// between probe-sourced MaxHead and the actual chain tip without
	// changing eligibility for queries within the probed window.
	if h := k.HeightOrZero(); h > head {
		head = h
	}

	type scored struct {
		b     *backend.Backend
		score float64
	}
	picks := make([]scored, 0, len(all))
	for _, b := range all {
		if !b.Has(k.Protocol) {
			continue
		}
		if s.registry.IsDrained(b.Name) {
			continue
		}
		if k.Class == types.ClassByHeight && k.Height != nil {
			if !b.Coverage.Eligible(*k.Height, head) {
				continue
			}
		} else if k.Class == types.ClassLatest || k.Class == types.ClassByHash {
			// latest / by-hash: any backend that follows head works; bounded
			// only if its upper covers head.
			if !b.Coverage.Eligible(0, head) {
				continue
			}
		}
		// Health gate: take the first snapshot along the protocol's witness
		// chain (see healthProtocols).
		var (
			hs    health.Snapshot
			found bool
		)
		for _, hp := range healthProtocols(k.Protocol) {
			if hs, found = s.health.Get(b.Name, hp); found {
				break
			}
		}
		if !found {
			// No prober covers this backend at all — e.g. eth_rpc-only
			// with neither an rpc endpoint (RPC prober) nor an eth_ws
			// endpoint (WS head prober). Treat as healthy rather than
			// unroutable.
			hs = health.Snapshot{Healthy: true}
		}
		if !hs.Healthy {
			continue
		}
		if s.maxLag > 0 && hs.Lag > s.maxLag {
			continue
		}
		// Circuit gate
		if !s.circuit.Allow(b.Name, k.Protocol) {
			continue
		}
		picks = append(picks, scored{b: b, score: s.score(b, hs, head)})
	}
	sort.Slice(picks, func(i, j int) bool { return picks[i].score > picks[j].score })

	out := make([]*backend.Backend, len(picks))
	for i, p := range picks {
		out[i] = p.b
	}
	return out
}

// score is higher = better candidate. Only healthy backends reach here
// (unhealthy ones are filtered in Candidates), so health is not a term.
func (s *RangeSelector) score(b *backend.Backend, hs health.Snapshot, head int64) float64 {
	specificity := specificityScore(b.Coverage, head)
	lagPenalty := 0.0
	if s.maxLag > 0 {
		lagPenalty = float64(hs.Lag) / float64(s.maxLag)
	}
	weight := float64(b.Weight) / 100.0
	return weight * (s.weights.Specificity*specificity - s.weights.Lag*lagPenalty)
}

// specificityScore returns 1.0 for the narrowest coverage (a tiny pruned
// window) and approaches 0 for archive. The selector favors narrower
// backends so that archives are spared cheap historical reads.
func specificityScore(c backend.Coverage, head int64) float64 {
	if head <= 0 {
		head = 1
	}
	span := c.EffectiveUpper(head) - c.EffectiveLower(head) + 1
	if span <= 0 {
		span = 1
	}
	// Map span∈[1, head] → score∈[1, ~0]. Use 1 - span/head, floor at 0.05
	// so archives still rank above outright-broken backends.
	s := 1 - float64(span)/float64(head)
	if s < 0.05 {
		return 0.05
	}
	return s
}

// evmWitnessProtocols is the health-witness chain shared by the EVM/stream
// request protocols. Package-level so the hot-path lookup doesn't allocate.
var evmWitnessProtocols = []types.Protocol{types.ProtoRPC, types.ProtoEthWS}

// healthProtocols ranks the snapshot protocols that can witness a request
// protocol's health, most authoritative first; the first snapshot found
// wins. EthRPC/EthWS/ChainStream all ride the same chain-head signal:
// the CometBFT RPC prober (ProtoRPC) owns the authoritative height/lag
// numbers, so its snapshot takes precedence when present; the eth_ws head
// prober (ProtoEthWS) is the fallback witness for EVM-only backends that
// expose no rpc endpoint and would otherwise never be health-gated. WS
// snapshots carry no Lag (lag stays an RPC-prober concern) — when the WS
// snapshot is the witness, stream liveness itself is the freshness signal.
func healthProtocols(p types.Protocol) []types.Protocol {
	switch p {
	case types.ProtoEthRPC, types.ProtoEthWS, types.ProtoChainStream:
		return evmWitnessProtocols
	default:
		return []types.Protocol{p}
	}
}
