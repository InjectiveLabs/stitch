package config

import (
	"errors"
	"fmt"
)

// Validate runs semantic checks beyond YAML parsing.
func Validate(c *Config) error {
	if c == nil {
		return errors.New("nil config")
	}
	if err := validateLog(c.Log); err != nil {
		return err
	}
	if err := validateBackends(c.Backends); err != nil {
		return err
	}
	if err := validatePolicies(c.Policies); err != nil {
		return err
	}
	return nil
}

func validateLog(l LogConfig) error {
	switch l.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log.level=%q (allowed: debug|info|warn|error)", l.Level)
	}
	switch l.Format {
	case "json", "text":
	default:
		return fmt.Errorf("log.format=%q (allowed: json|text)", l.Format)
	}
	return nil
}

func validateBackends(bs []BackendConfig) error {
	if len(bs) == 0 {
		return errors.New("backends: at least one required")
	}
	seen := make(map[string]struct{}, len(bs))
	for i, b := range bs {
		if b.Name == "" {
			return fmt.Errorf("backends[%d]: name is required", i)
		}
		if _, dup := seen[b.Name]; dup {
			return fmt.Errorf("backends[%d]: duplicate name %q", i, b.Name)
		}
		seen[b.Name] = struct{}{}
		if err := validateCoverage(b.Coverage); err != nil {
			return fmt.Errorf("backends[%d] %q: %w", i, b.Name, err)
		}
		if len(b.Endpoints) == 0 {
			return fmt.Errorf("backends[%d] %q: at least one endpoint required", i, b.Name)
		}
		for k := range b.Endpoints {
			if _, ok := KnownEndpointKeys[k]; !ok {
				return fmt.Errorf("backends[%d] %q: unknown endpoint key %q", i, b.Name, k)
			}
		}
		if b.Weight < 0 {
			return fmt.Errorf("backends[%d] %q: weight must be ≥ 0", i, b.Name)
		}
	}
	return nil
}

func validateCoverage(c Coverage) error {
	switch c.Kind {
	case CoverageArchive:
		if c.Lower != 0 || c.Upper != 0 || c.Keep != 0 {
			return errors.New("coverage.kind=archive: lower/upper/keep must be unset")
		}
	case CoverageBounded:
		if c.Lower < 1 {
			return errors.New("coverage.kind=bounded: lower must be ≥ 1")
		}
		if c.Upper < c.Lower {
			return errors.New("coverage.kind=bounded: upper must be ≥ lower")
		}
		if c.Keep != 0 {
			return errors.New("coverage.kind=bounded: keep must be unset")
		}
	case CoverageOpen:
		if c.Lower < 1 {
			return errors.New("coverage.kind=open: lower must be ≥ 1")
		}
		if c.Upper != 0 || c.Keep != 0 {
			return errors.New("coverage.kind=open: upper/keep must be unset")
		}
	case CoveragePruned:
		if c.Keep < 1 {
			return errors.New("coverage.kind=pruned: keep must be ≥ 1")
		}
		if c.Lower != 0 || c.Upper != 0 {
			return errors.New("coverage.kind=pruned: lower/upper must be unset")
		}
	default:
		return fmt.Errorf("coverage.kind=%q (allowed: archive|bounded|open|pruned)", c.Kind)
	}
	return nil
}

func validatePolicies(p PoliciesConfig) error {
	if p.Failover.MaxAttempts < 1 {
		return errors.New("policies.failover.max_attempts must be ≥ 1")
	}
	if p.Failover.PerAttemptTimeout <= 0 {
		return errors.New("policies.failover.per_attempt_timeout must be > 0")
	}
	if p.Circuit.ErrorThreshold <= 0 || p.Circuit.ErrorThreshold > 1 {
		return errors.New("policies.circuit.error_threshold must be in (0, 1]")
	}
	if p.Circuit.MinRequests < 1 {
		return errors.New("policies.circuit.min_requests must be ≥ 1")
	}
	if p.Circuit.OpenDuration <= 0 {
		return errors.New("policies.circuit.open_duration must be > 0")
	}
	if p.Health.ProbeInterval <= 0 {
		return errors.New("policies.health.probe_interval must be > 0")
	}
	if p.Health.MaxLagBlocks < 0 {
		return errors.New("policies.health.max_lag_blocks must be ≥ 0")
	}
	switch p.Subscriptions.SlowConsumer {
	case "", "drop", "disconnect", "backpressure":
	default:
		return fmt.Errorf("policies.subscriptions.slow_consumer=%q (allowed: drop|disconnect|backpressure)", p.Subscriptions.SlowConsumer)
	}
	switch p.Cache.L2Kind {
	case "", "none", "redis":
	default:
		return fmt.Errorf("policies.cache.l2_kind=%q (allowed: none|redis)", p.Cache.L2Kind)
	}
	return nil
}
