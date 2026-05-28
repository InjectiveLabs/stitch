// Package selector picks an ordered list of backend candidates for a given
// route. The default RangeSelector scores by specificity, health, and
// circuit state.
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
// archive nodes are spared cheap historical reads.
type Weights struct {
	Specificity float64
	Health      float64
	CircuitOpen float64
	Lag         float64
}

func DefaultWeights() Weights {
	return Weights{
		Specificity: 1.0,
		Health:      0.6,
		CircuitOpen: 1000.0, // dominant penalty: practically excludes open circuits
		Lag:         0.2,
	}
}

// RangeSelector implements Selector based on backend coverage + health.
type RangeSelector struct {
	registry  *backend.Registry
	health    *health.Registry
	circuit   *circuit.Manager
	weights   Weights
	maxLag    int64
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
	if head == 0 {
		head = k.HeightOrZero()
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
		// Health gate
		hs, ok := s.health.Get(b.Name, healthProtocol(k.Protocol))
		if !ok {
			// No probe yet: treat as healthy, score conservatively.
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

// score is higher = better candidate.
func (s *RangeSelector) score(b *backend.Backend, hs health.Snapshot, head int64) float64 {
	specificity := specificityScore(b.Coverage, head)
	healthScore := 0.0
	if hs.Healthy {
		healthScore = 1.0
	}
	lagPenalty := 0.0
	if s.maxLag > 0 {
		lagPenalty = float64(hs.Lag) / float64(s.maxLag)
	}
	weight := float64(b.Weight) / 100.0
	return weight*(s.weights.Specificity*specificity+
		s.weights.Health*healthScore-
		s.weights.Lag*lagPenalty)
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

// healthProtocol maps the request protocol to the protocol whose health
// snapshot is most informative. EthRPC/EthWS/ChainStream all share a
// chain-head signal that the RPC prober produces today.
func healthProtocol(p types.Protocol) types.Protocol {
	switch p {
	case types.ProtoEthRPC, types.ProtoEthWS, types.ProtoChainStream:
		return types.ProtoRPC
	default:
		return p
	}
}
