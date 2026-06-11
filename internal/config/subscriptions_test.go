package config

import (
	"strings"
	"testing"
	"time"
)

func TestSubscriptionsDefaults(t *testing.T) {
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
	if err := Validate(cfg); err != nil {
		t.Fatalf("validate: %v", err)
	}
	s := cfg.Policies.Subscriptions
	if s.Multicast {
		t.Error("multicast must default to false (opt-in)")
	}
	if s.SlowConsumer != "drop" {
		t.Errorf("default slow_consumer: got %q", s.SlowConsumer)
	}
	if s.SendBuffer != 64 {
		t.Errorf("default send_buffer: got %d", s.SendBuffer)
	}
	if s.ReplayTimeout != 30*time.Second {
		t.Errorf("default replay_timeout: got %v", s.ReplayTimeout)
	}
}

func TestSubscriptionsKnobsRoundTrip(t *testing.T) {
	yaml := `
policies:
  subscriptions:
    multicast: true
    slow_consumer: disconnect
    send_buffer: 128
    replay_timeout: 90s
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
	s := cfg.Policies.Subscriptions
	if !s.Multicast || s.SlowConsumer != "disconnect" || s.SendBuffer != 128 || s.ReplayTimeout != 90*time.Second {
		t.Errorf("subscriptions knobs round-trip: %+v", s)
	}
}

// TestSubscriptionsSendBufferRejectsNegative: a configured send_buffer
// must be ≥ 1 — zero is "unset" (defaulted to 64 before validation), and
// negatives are rejected outright.
func TestSubscriptionsSendBufferRejectsNegative(t *testing.T) {
	yaml := `
policies:
  subscriptions:
    send_buffer: -1
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
	err = Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "send_buffer") {
		t.Fatalf("expected send_buffer error, got %v", err)
	}
}
