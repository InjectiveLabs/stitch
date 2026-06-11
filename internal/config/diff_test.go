package config

import (
	"reflect"
	"testing"
	"time"
)

func diffFixture() *Config {
	return &Config{
		Listen: ListenConfig{
			RPC:   AddrConfig{Addr: ":26657"},
			Admin: AddrConfig{Addr: ":9091"},
		},
		Log: LogConfig{Level: "info", Format: "json"},
		Policies: PoliciesConfig{
			Failover:         FailoverPolicy{MaxAttempts: 3, PerAttemptTimeout: 5 * time.Second},
			Hedging:          HedgingPolicy{Enabled: true, Methods: []string{"eth_call"}, HedgeAfter: 200 * time.Millisecond},
			Circuit:          CircuitPolicy{ErrorThreshold: 0.5, MinRequests: 20, OpenDuration: 30 * time.Second},
			Cache:            CachePolicy{Enabled: true, ConfirmationDepth: 100, TTL: 5 * time.Minute, HashIndexEntries: 100_000, ResponseEntries: 50_000},
			Health:           HealthPolicy{ProbeInterval: 5 * time.Second, MaxLagBlocks: 50},
			Subscriptions:    SubscriptionsPolicy{Multicast: true, SlowConsumer: "drop", ReplayTimeout: 30 * time.Second},
			DangerousMethods: DangerousMethodsPolicy{Allow: []string{"debug_traceCall"}},
		},
		Backends: []BackendConfig{{
			Name:      "a",
			Coverage:  Coverage{Kind: CoverageArchive},
			Weight:    100,
			Endpoints: map[string]string{"rpc": "http://x:26657"},
		}},
	}
}

func TestDiffNonReloadableNoChange(t *testing.T) {
	if got := DiffNonReloadable(diffFixture(), diffFixture()); len(got) != 0 {
		t.Fatalf("identical configs should diff empty; got %v", got)
	}
}

func TestDiffNonReloadableDetectsEachSection(t *testing.T) {
	cases := []struct {
		section string
		mutate  func(*Config)
	}{
		{"listen", func(c *Config) { c.Listen.RPC.Addr = ":1234" }},
		{"policies.failover", func(c *Config) { c.Policies.Failover.MaxAttempts = 9 }},
		{"policies.hedging", func(c *Config) { c.Policies.Hedging.Enabled = false }},
		{"policies.circuit", func(c *Config) { c.Policies.Circuit.MinRequests = 1 }},
		{"policies.cache", func(c *Config) { c.Policies.Cache.TTL = time.Minute }},
		{"policies.health", func(c *Config) { c.Policies.Health.MaxLagBlocks = 5 }},
		{"policies.subscriptions", func(c *Config) { c.Policies.Subscriptions.SlowConsumer = "disconnect" }},
		{"policies.dangerous_methods", func(c *Config) { c.Policies.DangerousMethods.Allow = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.section, func(t *testing.T) {
			next := diffFixture()
			tc.mutate(next)
			got := DiffNonReloadable(diffFixture(), next)
			if !reflect.DeepEqual(got, []string{tc.section}) {
				t.Fatalf("got %v; want [%s]", got, tc.section)
			}
		})
	}
}

func TestDiffNonReloadableIgnoresReloadableSections(t *testing.T) {
	next := diffFixture()
	next.Log.Level = "debug"
	next.Backends = append(next.Backends, BackendConfig{
		Name:      "b",
		Coverage:  Coverage{Kind: CoverageArchive},
		Weight:    100,
		Endpoints: map[string]string{"rpc": "http://y:26657"},
	})
	if got := DiffNonReloadable(diffFixture(), next); len(got) != 0 {
		t.Fatalf("log/backends changes DO reload and must not be reported; got %v", got)
	}
}

func TestDiffNonReloadableMultipleSections(t *testing.T) {
	next := diffFixture()
	next.Listen.Admin.Addr = ":9999"
	next.Policies.Circuit.OpenDuration = time.Minute
	got := DiffNonReloadable(diffFixture(), next)
	if !reflect.DeepEqual(got, []string{"listen", "policies.circuit"}) {
		t.Fatalf("got %v; want [listen policies.circuit]", got)
	}
}
