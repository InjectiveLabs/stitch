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
			Subscriptions:    SubscriptionsPolicy{Multicast: true, SlowConsumer: "drop", ReplayTimeout: durPtr(30 * time.Second)},
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

// TestDiffNonReloadableBootBaseline encodes the caller contract: prev is
// always the STARTUP config. Non-reloadable sections run with boot values
// forever, so an edited-then-reverted file must diff clean against boot
// (no false warning), and an edit must keep warning on every subsequent
// reload, not just the first one after the change.
func TestDiffNonReloadableBootBaseline(t *testing.T) {
	boot := diffFixture()

	edited := diffFixture()
	edited.Policies.Circuit.MinRequests = 99

	// First reload after the edit warns...
	if got := DiffNonReloadable(boot, edited); !reflect.DeepEqual(got, []string{"policies.circuit"}) {
		t.Fatalf("edit vs boot: got %v; want [policies.circuit]", got)
	}
	// ...and so does every later reload while the file still differs from
	// boot — the baseline must NOT advance to the last-loaded config (a
	// last-loaded baseline would return empty here).
	stillEdited := diffFixture()
	stillEdited.Policies.Circuit.MinRequests = 99
	if got := DiffNonReloadable(boot, stillEdited); !reflect.DeepEqual(got, []string{"policies.circuit"}) {
		t.Fatalf("unchanged edit vs boot: got %v; want [policies.circuit]", got)
	}

	// Reverting the file to boot values silences the warning — even though
	// it differs from the previously loaded (edited) config.
	reverted := diffFixture()
	if got := DiffNonReloadable(boot, reverted); len(got) != 0 {
		t.Fatalf("revert-to-boot must not warn; got %v", got)
	}
}

// TestDiffNonReloadableClassifiesEveryField is the rot guard: every field
// of Config and PoliciesConfig must be explicitly classified. Adding a
// config section without deciding whether DiffNonReloadable should compare
// it fails this test.
//
//	reloadable — applied live by the reload closure (excluded from diff)
//	diffed     — compared by DiffNonReloadable (warned as ignored-until-restart)
//	dead       — parsed but not wired to anything yet; classify on implementation
func TestDiffNonReloadableClassifiesEveryField(t *testing.T) {
	classification := map[string]string{
		// Config
		"Listen":   "diffed",
		"Log":      "reloadable",
		"Policies": "diffed", // per-field below
		"Backends": "reloadable",
		"Auth":     "dead", // phase 8: classify on implementation
		// PoliciesConfig
		"Policies.Failover":         "diffed",
		"Policies.Hedging":          "diffed",
		"Policies.Circuit":          "diffed",
		"Policies.Cache":            "diffed",
		"Policies.Health":           "diffed",
		"Policies.Subscriptions":    "diffed",
		"Policies.DangerousMethods": "diffed",
	}
	valid := map[string]bool{"reloadable": true, "diffed": true, "dead": true}

	seen := map[string]bool{}
	check := func(typ reflect.Type, prefix string) {
		t.Helper()
		for i := 0; i < typ.NumField(); i++ {
			name := prefix + typ.Field(i).Name
			seen[name] = true
			class, ok := classification[name]
			if !ok {
				t.Errorf("config field %s is not classified — decide whether DiffNonReloadable must compare it (reloadable | diffed | dead) and add it to this map", name)
				continue
			}
			if !valid[class] {
				t.Errorf("config field %s has invalid classification %q", name, class)
			}
		}
	}
	check(reflect.TypeOf(Config{}), "")
	check(reflect.TypeOf(PoliciesConfig{}), "Policies.")

	// Stale entries rot too: every classified name must still exist.
	for name := range classification {
		if !seen[name] {
			t.Errorf("classification entry %s no longer matches any config field — remove or rename it", name)
		}
	}
}
