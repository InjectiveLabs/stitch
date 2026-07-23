// Package backend defines the runtime Backend type — built from a
// config.BackendConfig and used by the selector/forwarder/health stack.
package backend

import (
	"errors"
	"fmt"

	"github.com/InjectiveLabs/stitch/internal/config"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// Backend is one upstream node, including which heights it serves and which
// per-protocol endpoint URLs to dial.
type Backend struct {
	Name      string
	Coverage  Coverage
	Weight    int
	Tags      []string
	Endpoints map[types.Protocol]string
}

// Has reports whether the backend serves the given protocol.
func (b *Backend) Has(p types.Protocol) bool {
	_, ok := b.Endpoints[p]
	return ok
}

// Endpoint returns the configured upstream URL for protocol p, or the empty
// string if not configured.
func (b *Backend) Endpoint(p types.Protocol) string { return b.Endpoints[p] }

// CoverageKind discriminates the four shapes of upstream coverage.
type CoverageKind uint8

const (
	CovArchive CoverageKind = iota
	CovBounded
	CovOpen
	CovPruned
)

func (k CoverageKind) String() string {
	switch k {
	case CovArchive:
		return config.CoverageArchive
	case CovBounded:
		return config.CoverageBounded
	case CovOpen:
		return config.CoverageOpen
	case CovPruned:
		return config.CoveragePruned
	default:
		return "unknown"
	}
}

// Coverage is the runtime form of config.Coverage. Cheap to copy.
type Coverage struct {
	Kind  CoverageKind
	Lower int64 // bounded | open
	Upper int64 // bounded
	Keep  int64 // pruned
}

// Eligible reports whether this backend serves block height h, given the
// current observed head (used only for pruned backends).
func (c Coverage) Eligible(h, head int64) bool {
	if h <= 0 {
		// "latest": every kind that follows head is eligible.
		return c.Kind != CovBounded || c.Upper >= head
	}
	switch c.Kind {
	case CovArchive:
		return h >= 1 && h <= head
	case CovBounded:
		return h >= c.Lower && h <= c.Upper
	case CovOpen:
		return h >= c.Lower && h <= head
	case CovPruned:
		if c.Keep <= 0 {
			return false
		}
		floor := head - c.Keep + 1
		if floor < 1 {
			floor = 1
		}
		return h >= floor && h <= head
	}
	return false
}

// EffectiveLower returns the smallest height this backend can serve given
// the observed head. Used for diagnostics and for the selector's specificity
// score.
func (c Coverage) EffectiveLower(head int64) int64 {
	switch c.Kind {
	case CovArchive:
		return 1
	case CovBounded, CovOpen:
		return c.Lower
	case CovPruned:
		floor := head - c.Keep + 1
		if floor < 1 {
			return 1
		}
		return floor
	}
	return 0
}

// EffectiveUpper returns the largest height this backend can serve given
// the observed head.
func (c Coverage) EffectiveUpper(head int64) int64 {
	switch c.Kind {
	case CovArchive, CovOpen, CovPruned:
		return head
	case CovBounded:
		return c.Upper
	}
	return 0
}

// FromConfig builds a runtime Backend list from a parsed config.
func FromConfig(cfgs []config.BackendConfig) ([]*Backend, error) {
	out := make([]*Backend, 0, len(cfgs))
	for i, c := range cfgs {
		cov, err := coverageFromConfig(c.Coverage)
		if err != nil {
			return nil, fmt.Errorf("backends[%d] %q: %w", i, c.Name, err)
		}
		eps := make(map[types.Protocol]string, len(c.Endpoints))
		for k, v := range c.Endpoints {
			eps[types.Protocol(k)] = v
		}
		out = append(out, &Backend{
			Name:      c.Name,
			Coverage:  cov,
			Weight:    c.Weight,
			Tags:      append([]string(nil), c.Tags...),
			Endpoints: eps,
		})
	}
	return out, nil
}

func coverageFromConfig(c config.Coverage) (Coverage, error) {
	switch c.Kind {
	case config.CoverageArchive:
		return Coverage{Kind: CovArchive}, nil
	case config.CoverageBounded:
		return Coverage{Kind: CovBounded, Lower: c.Lower, Upper: c.Upper}, nil
	case config.CoverageOpen:
		return Coverage{Kind: CovOpen, Lower: c.Lower}, nil
	case config.CoveragePruned:
		return Coverage{Kind: CovPruned, Keep: c.Keep}, nil
	default:
		return Coverage{}, errors.New("unknown coverage kind: " + c.Kind)
	}
}
