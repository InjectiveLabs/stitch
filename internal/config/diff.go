package config

import "reflect"

// DiffNonReloadable returns the names of config sections that differ
// between prev and next but are only read at startup, so a hot reload
// silently ignores them until restart. Backends and log are excluded —
// those genuinely reload.
func DiffNonReloadable(prev, next *Config) []string {
	var out []string
	add := func(name string, a, b any) {
		if !reflect.DeepEqual(a, b) {
			out = append(out, name)
		}
	}
	add("listen", prev.Listen, next.Listen)
	add("policies.failover", prev.Policies.Failover, next.Policies.Failover)
	add("policies.hedging", prev.Policies.Hedging, next.Policies.Hedging)
	add("policies.circuit", prev.Policies.Circuit, next.Policies.Circuit)
	add("policies.cache", prev.Policies.Cache, next.Policies.Cache)
	add("policies.health", prev.Policies.Health, next.Policies.Health)
	add("policies.subscriptions", prev.Policies.Subscriptions, next.Policies.Subscriptions)
	add("policies.dangerous_methods", prev.Policies.DangerousMethods, next.Policies.DangerousMethods)
	return out
}
