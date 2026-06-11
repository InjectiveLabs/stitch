package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseValidMinimal(t *testing.T) {
	yaml := `
listen:
  admin: { addr: "127.0.0.1:9091" }
backends:
  - name: a
    coverage: { kind: archive }
    endpoints:
      rpc: http://x:26657
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	applyDefaults(cfg)
	if err := Validate(cfg); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Policies.Failover.MaxAttempts != 3 {
		t.Errorf("default MaxAttempts: got %d", cfg.Policies.Failover.MaxAttempts)
	}
	if cfg.Policies.Health.ProbeInterval != 5*time.Second {
		t.Errorf("default ProbeInterval: got %v", cfg.Policies.Health.ProbeInterval)
	}
	if cfg.Backends[0].Weight != 100 {
		t.Errorf("default backend weight: got %d", cfg.Backends[0].Weight)
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	yaml := `
backends:
  - name: a
    coverage: { kind: archive }
    endpoints: { rpc: x }
mystery_key: yes
`
	if _, err := Parse([]byte(yaml)); err == nil {
		t.Fatal("expected parse to reject unknown top-level key")
	}
}

func TestValidateRejectsBadCoverage(t *testing.T) {
	cases := map[string]string{
		"empty kind":            `{kind: ""}`,
		"archive with bounds":   `{kind: archive, lower: 1}`,
		"bounded inverted":      `{kind: bounded, lower: 100, upper: 50}`,
		"bounded missing lower": `{kind: bounded, upper: 50}`,
		"open with upper":       `{kind: open, lower: 1, upper: 50}`,
		"pruned without keep":   `{kind: pruned}`,
		"pruned with lower":     `{kind: pruned, keep: 100, lower: 1}`,
		"unknown kind":          `{kind: weird}`,
	}
	for name, cov := range cases {
		t.Run(name, func(t *testing.T) {
			yaml := "backends:\n  - name: a\n    coverage: " + cov + "\n    endpoints: { rpc: x }\n"
			cfg, err := Parse([]byte(yaml))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			applyDefaults(cfg)
			if err := Validate(cfg); err == nil {
				t.Fatalf("validate accepted bad coverage %q", cov)
			}
		})
	}
}

func TestValidateRejectsDuplicateBackendNames(t *testing.T) {
	yaml := `
backends:
  - name: a
    coverage: { kind: archive }
    endpoints: { rpc: x }
  - name: a
    coverage: { kind: archive }
    endpoints: { rpc: y }
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	applyDefaults(cfg)
	err = Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate-name error, got %v", err)
	}
}

func TestValidateRejectsUnknownEndpointKey(t *testing.T) {
	yaml := `
backends:
  - name: a
    coverage: { kind: archive }
    endpoints: { rpc: x, mystery: y }
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	applyDefaults(cfg)
	err = Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "mystery") {
		t.Fatalf("expected unknown-endpoint error, got %v", err)
	}
}

func TestDefaultsHedgingAndCache(t *testing.T) {
	yaml := `
backends:
  - name: a
    coverage: { kind: archive }
    endpoints: { rpc: x }
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	applyDefaults(cfg)
	if cfg.Policies.Hedging.HedgeAfter != 200*time.Millisecond {
		t.Errorf("default hedge_after: got %v", cfg.Policies.Hedging.HedgeAfter)
	}
	if cfg.Policies.Cache.TTL != 5*time.Minute {
		t.Errorf("default cache ttl: got %v", cfg.Policies.Cache.TTL)
	}
	if cfg.Policies.Cache.HashIndexEntries != 100_000 {
		t.Errorf("default hash_index_entries: got %d", cfg.Policies.Cache.HashIndexEntries)
	}
	if cfg.Policies.Cache.ResponseEntries != 50_000 {
		t.Errorf("default response_entries: got %d", cfg.Policies.Cache.ResponseEntries)
	}
}

func TestParseHedgingAndCacheKnobsRoundTrip(t *testing.T) {
	yaml := `
policies:
  hedging:
    enabled: true
    methods: [eth_call]
    hedge_after: 150ms
  cache:
    enabled: true
    ttl: 90s
    hash_index_entries: 1234
    response_entries: 567
backends:
  - name: a
    coverage: { kind: archive }
    endpoints: { rpc: x }
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	applyDefaults(cfg)
	if err := Validate(cfg); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Policies.Hedging.HedgeAfter != 150*time.Millisecond {
		t.Errorf("hedge_after: got %v", cfg.Policies.Hedging.HedgeAfter)
	}
	if cfg.Policies.Cache.TTL != 90*time.Second {
		t.Errorf("cache ttl: got %v", cfg.Policies.Cache.TTL)
	}
	if cfg.Policies.Cache.HashIndexEntries != 1234 {
		t.Errorf("hash_index_entries: got %d", cfg.Policies.Cache.HashIndexEntries)
	}
	if cfg.Policies.Cache.ResponseEntries != 567 {
		t.Errorf("response_entries: got %d", cfg.Policies.Cache.ResponseEntries)
	}
}

func TestValidateHedgingAndCacheBounds(t *testing.T) {
	base := func() *Config {
		return &Config{
			Backends: []BackendConfig{{
				Name:      "a",
				Coverage:  Coverage{Kind: CoverageArchive},
				Endpoints: map[string]string{"rpc": "x"},
			}},
			Log: LogConfig{Level: "info", Format: "json"},
			Policies: PoliciesConfig{
				Failover: FailoverPolicy{MaxAttempts: 3, PerAttemptTimeout: time.Second},
				Circuit:  CircuitPolicy{ErrorThreshold: 0.5, MinRequests: 1, OpenDuration: time.Second},
				Health:   HealthPolicy{ProbeInterval: time.Second},
				Hedging:  HedgingPolicy{Enabled: true, HedgeAfter: 200 * time.Millisecond},
				Cache:    CachePolicy{Enabled: true, TTL: time.Minute, HashIndexEntries: 10, ResponseEntries: 10},
			},
		}
	}

	cases := []struct {
		name    string
		mutate  func(*Config)
		errPart string
	}{
		{"hedge_after negative while enabled", func(c *Config) { c.Policies.Hedging.HedgeAfter = -time.Second }, "hedge_after"},
		{"cache ttl negative while enabled", func(c *Config) { c.Policies.Cache.TTL = -time.Second }, "cache.ttl"},
		{"hash_index_entries negative while enabled", func(c *Config) { c.Policies.Cache.HashIndexEntries = -1 }, "hash_index_entries"},
		{"response_entries negative while enabled", func(c *Config) { c.Policies.Cache.ResponseEntries = -1 }, "response_entries"},
		// Both cache structures are built unconditionally at startup, so the
		// capacity bounds hold even with cache.enabled=false.
		{"hash_index_entries zero while disabled", func(c *Config) {
			c.Policies.Cache = CachePolicy{Enabled: false, ResponseEntries: 10}
		}, "hash_index_entries"},
		{"response_entries negative while disabled", func(c *Config) {
			c.Policies.Cache = CachePolicy{Enabled: false, HashIndexEntries: 10, ResponseEntries: -1}
		}, "response_entries"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(cfg)
			err := Validate(cfg)
			if err == nil || !strings.Contains(err.Error(), tc.errPart) {
				t.Fatalf("expected error containing %q, got %v", tc.errPart, err)
			}
		})
	}

	// Disabled hedging tolerates zero values, and disabled cache tolerates a
	// zero TTL — but the capacities still apply (the structures are built
	// regardless of cache.enabled).
	cfg := base()
	cfg.Policies.Hedging = HedgingPolicy{}
	cfg.Policies.Cache = CachePolicy{HashIndexEntries: 10, ResponseEntries: 10}
	if err := Validate(cfg); err != nil {
		t.Fatalf("disabled hedging/zero-ttl cache should pass with sane capacities: %v", err)
	}
}

func TestValidatePolicyBounds(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{{
			Name:      "a",
			Coverage:  Coverage{Kind: CoverageArchive},
			Endpoints: map[string]string{"rpc": "x"},
		}},
		Log: LogConfig{Level: "info", Format: "json"},
		Policies: PoliciesConfig{
			Failover: FailoverPolicy{MaxAttempts: 0, PerAttemptTimeout: time.Second},
			Circuit:  CircuitPolicy{ErrorThreshold: 0.5, MinRequests: 1, OpenDuration: time.Second},
			Health:   HealthPolicy{ProbeInterval: time.Second},
		},
	}
	if err := Validate(cfg); err == nil || !strings.Contains(err.Error(), "max_attempts") {
		t.Fatalf("expected max_attempts error, got %v", err)
	}
}
