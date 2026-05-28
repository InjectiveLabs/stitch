package config

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Load reads, parses, and validates the YAML config at path. Strict mode
// rejects unknown keys so typos surface early.
//
// Environment variables are expanded after read but before parse:
// `${FOO}` and `$FOO` in the YAML are replaced with the value of the
// process environment variable FOO (or empty if unset). Stitch
// deliberately does not require fallbacks (`${FOO:-default}`); operators
// who need defaults should set the variable explicitly.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	expanded := os.ExpandEnv(string(data))
	cfg, err := Parse([]byte(expanded))
	if err != nil {
		return nil, err
	}
	applyDefaults(cfg)
	if err := Validate(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Parse decodes YAML bytes into a Config without applying defaults or
// validation. Useful for tests.
func Parse(data []byte) (*Config, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	cfg := &Config{}
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

func applyDefaults(c *Config) {
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "json"
	}
	if c.Policies.Failover.MaxAttempts == 0 {
		c.Policies.Failover.MaxAttempts = 3
	}
	if c.Policies.Failover.PerAttemptTimeout == 0 {
		c.Policies.Failover.PerAttemptTimeout = 5 * time.Second
	}
	if c.Policies.Circuit.ErrorThreshold == 0 {
		c.Policies.Circuit.ErrorThreshold = 0.5
	}
	if c.Policies.Circuit.MinRequests == 0 {
		c.Policies.Circuit.MinRequests = 20
	}
	if c.Policies.Circuit.OpenDuration == 0 {
		c.Policies.Circuit.OpenDuration = 30 * time.Second
	}
	if c.Policies.Cache.ConfirmationDepth == 0 {
		c.Policies.Cache.ConfirmationDepth = 100
	}
	if c.Policies.Cache.L1SizeMB == 0 {
		c.Policies.Cache.L1SizeMB = 1024
	}
	if c.Policies.Cache.L2Kind == "" {
		c.Policies.Cache.L2Kind = "none"
	}
	if c.Policies.Health.ProbeInterval == 0 {
		c.Policies.Health.ProbeInterval = 5 * time.Second
	}
	if c.Policies.Health.MaxLagBlocks == 0 {
		c.Policies.Health.MaxLagBlocks = 50
	}
	if c.Policies.Subscriptions.SlowConsumer == "" {
		c.Policies.Subscriptions.SlowConsumer = "drop"
	}
	if c.Policies.Subscriptions.ReplayTimeout == 0 {
		c.Policies.Subscriptions.ReplayTimeout = 30 * time.Second
	}
	for i := range c.Backends {
		if c.Backends[i].Weight == 0 {
			c.Backends[i].Weight = 100
		}
	}
}
