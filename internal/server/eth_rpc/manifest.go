// Package eth_rpc is the EVM JSON-RPC HTTP listener (server #5 in the
// Injective query reference).
//
// The package ships a YAML manifest that classifies every JSON-RPC method
// across eth, web3, net, txpool, personal, debug, miner, and inj
// namespaces. The decoder consults the manifest to produce a typed
// RouteKey; the forwarder turns that into an upstream call.
package eth_rpc

import (
	_ "embed"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

//go:embed manifest.yaml
var manifestYAML []byte

// Spec describes how one JSON-RPC method is routed.
//
// Mutually-exclusive triggers (in order of precedence):
//   1. Subscription: rejected on HTTP listener (handled by ws hub in phase 5).
//   2. Broadcast: fan out to all healthy backends (phase 6); else single attempt.
//   3. StickyFilter: filter id pinned to the backend that minted it.
//   4. Stateless: no upstream call required; still proxied today.
//   5. HashParam set: hash-keyed routing (memo cache in phase 6).
//   6. HeightParam set with Kind: height-keyed routing.
//   7. Height: "latest": every backend whose head we trust.
type Spec struct {
	Height               string `yaml:"height,omitempty"` // only "latest" is meaningful
	HeightParam          *int   `yaml:"height_param,omitempty"`
	HashParam            *int   `yaml:"hash_param,omitempty"`
	Kind                 string `yaml:"kind,omitempty"`
	Stateless            bool   `yaml:"stateless,omitempty"`
	Broadcast            bool   `yaml:"broadcast,omitempty"`
	Subscription         bool   `yaml:"subscription,omitempty"`
	Idempotent           *bool  `yaml:"idempotent,omitempty"`
	Cacheable            bool   `yaml:"cacheable,omitempty"`
	StateOverrideParam   *int   `yaml:"state_override_param,omitempty"`
	Hedge                bool   `yaml:"hedge,omitempty"`
	StickyFilter         bool   `yaml:"sticky_filter,omitempty"`
	FollowID             *int   `yaml:"follow_id,omitempty"`
	Hidden               bool   `yaml:"hidden,omitempty"`
}

// IsIdempotent collapses the *bool to a default that depends on context:
//   - explicit value wins
//   - broadcast methods default to false
//   - everything else defaults to true (read-leaning)
func (s Spec) IsIdempotent() bool {
	if s.Idempotent != nil {
		return *s.Idempotent
	}
	return !s.Broadcast
}

// Manifest is the parsed manifest.yaml. Built once at init.
type Manifest struct {
	specs map[string]Spec
}

// Lookup returns the spec for method name. Unknown methods get a permissive
// default (latest, idempotent=false) so they pass through but never retry.
func (m *Manifest) Lookup(name string) Spec {
	if s, ok := m.specs[name]; ok {
		return s
	}
	idemp := false
	return Spec{Height: "latest", Idempotent: &idemp}
}

// Has reports whether name is in the manifest. Used to detect typos in
// validation tools.
func (m *Manifest) Has(name string) bool {
	_, ok := m.specs[name]
	return ok
}

// Names returns every known method (sorted for stable output by the
// `stitch methods` subcommand).
func (m *Manifest) Names() []string {
	names := make([]string, 0, len(m.specs))
	for k := range m.specs {
		names = append(names, k)
	}
	return names
}

// loadManifest parses the embedded YAML into a Manifest. Called from init.
func loadManifest() (*Manifest, error) {
	var raw map[string]Spec
	if err := yaml.Unmarshal(manifestYAML, &raw); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	for name, spec := range raw {
		if err := validateSpec(name, spec); err != nil {
			return nil, err
		}
	}
	return &Manifest{specs: raw}, nil
}

// validateSpec enforces shape invariants the decoder relies on.
func validateSpec(name string, s Spec) error {
	flags := 0
	if s.Subscription {
		flags++
	}
	if s.Broadcast {
		flags++
	}
	if s.StickyFilter {
		flags++
	}
	if s.Stateless {
		flags++
	}
	if s.HashParam != nil {
		flags++
	}
	if s.HeightParam != nil {
		flags++
	}
	if s.Height == "latest" {
		flags++
	}
	if flags == 0 {
		return fmt.Errorf("manifest %q: must declare one of {height, height_param, hash_param, stateless, broadcast, subscription, sticky_filter}", name)
	}
	if s.Height != "" && s.Height != "latest" {
		return fmt.Errorf("manifest %q: height=%q (only \"latest\" is meaningful)", name, s.Height)
	}
	if s.HeightParam != nil && s.Kind == "" {
		return fmt.Errorf("manifest %q: height_param without kind", name)
	}
	if s.HashParam != nil && s.Kind == "" {
		return fmt.Errorf("manifest %q: hash_param without kind", name)
	}
	if s.Kind != "" {
		switch s.Kind {
		case "block_number", "block_number_or_hash", "block_hash", "tx_hash":
		default:
			return fmt.Errorf("manifest %q: kind=%q (allowed: block_number|block_number_or_hash|block_hash|tx_hash)", name, s.Kind)
		}
	}
	if !strings.Contains(name, "_") {
		return fmt.Errorf("manifest %q: not a JSON-RPC method name (missing namespace prefix)", name)
	}
	return nil
}

// DefaultManifest is the package-loaded manifest. Methods missing from it
// are treated permissively per Lookup.
var DefaultManifest *Manifest

func init() {
	m, err := loadManifest()
	if err != nil {
		panic("eth_rpc: " + err.Error())
	}
	DefaultManifest = m
}
