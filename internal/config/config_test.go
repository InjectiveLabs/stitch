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
		"empty kind":             `{kind: ""}`,
		"archive with bounds":    `{kind: archive, lower: 1}`,
		"bounded inverted":       `{kind: bounded, lower: 100, upper: 50}`,
		"bounded missing lower":  `{kind: bounded, upper: 50}`,
		"open with upper":        `{kind: open, lower: 1, upper: 50}`,
		"pruned without keep":    `{kind: pruned}`,
		"pruned with lower":      `{kind: pruned, keep: 100, lower: 1}`,
		"unknown kind":           `{kind: weird}`,
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
