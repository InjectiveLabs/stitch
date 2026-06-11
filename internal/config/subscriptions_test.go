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
	if s.ReplayTimeout == nil {
		t.Fatal("absent replay_timeout must be defaulted to a non-nil pointer")
	}
	if *s.ReplayTimeout != 30*time.Second {
		t.Errorf("default replay_timeout: got %v", *s.ReplayTimeout)
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
	if !s.Multicast || s.SlowConsumer != "disconnect" || s.SendBuffer != 128 ||
		s.ReplayTimeout == nil || *s.ReplayTimeout != 90*time.Second {
		t.Errorf("subscriptions knobs round-trip: %+v", s)
	}
}

// TestSubscriptionsReplayTimeoutZeroSemantics pins the pointer semantics
// that make `replay_timeout: 0` reachable: an absent key defaults to 30s,
// an explicit 0s SURVIVES defaulting (single dial pass per resume — the
// old plain-duration field coerced it to 30s because the yaml zero value
// is indistinguishable from absent), and negatives fail validation.
func TestSubscriptionsReplayTimeoutZeroSemantics(t *testing.T) {
	const backends = `
backends:
  - name: a
    coverage: { kind: archive }
    endpoints: { rpc: x }
`
	load := func(t *testing.T, yaml string) (*Config, error) {
		t.Helper()
		cfg, err := Parse([]byte(yaml))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		applyDefaults(cfg)
		return cfg, Validate(cfg)
	}

	t.Run("absent defaults to 30s", func(t *testing.T) {
		cfg, err := load(t, backends)
		if err != nil {
			t.Fatalf("validate: %v", err)
		}
		got := cfg.Policies.Subscriptions.ReplayTimeout
		if got == nil || *got != 30*time.Second {
			t.Errorf("absent replay_timeout: got %v; want 30s", got)
		}
	})

	t.Run("explicit zero kept", func(t *testing.T) {
		cfg, err := load(t, "policies:\n  subscriptions:\n    replay_timeout: 0s\n"+backends)
		if err != nil {
			t.Fatalf("validate: %v", err)
		}
		got := cfg.Policies.Subscriptions.ReplayTimeout
		if got == nil {
			t.Fatal("explicit replay_timeout: 0s came back nil")
		}
		if *got != 0 {
			t.Errorf("explicit replay_timeout: 0s: got %v; want 0 (single dial pass)", *got)
		}
	})

	t.Run("negative rejected", func(t *testing.T) {
		_, err := load(t, "policies:\n  subscriptions:\n    replay_timeout: -1s\n"+backends)
		if err == nil || !strings.Contains(err.Error(), "replay_timeout") {
			t.Fatalf("expected replay_timeout error, got %v", err)
		}
	})
}

// durPtr is a test helper for building SubscriptionsPolicy literals.
func durPtr(d time.Duration) *time.Duration { return &d }

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
