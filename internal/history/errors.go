// Package history identifies backend retention failures, not generic query errors.
package history

import "strings"

// Unavailable reports known Cosmos store-retention errors. Callers must also
// require an idempotent, explicitly historical request before trying another
// backend. A missing version says nothing about that backend's overall health.
func Unavailable(message string) bool {
	m := strings.ToLower(message)
	return (strings.Contains(m, "version does not exist") &&
		(strings.Contains(m, "iavl") || strings.Contains(m, "failed to load state") || strings.Contains(m, "failed to load version"))) ||
		(strings.Contains(m, "no commit info found") && strings.Contains(m, "version"))
}
