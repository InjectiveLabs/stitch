// Package runtime holds tiny cross-cutting helpers that don't deserve their
// own package.
package runtime

import "github.com/google/uuid"

// NewRequestID returns a UUIDv7 string. v7 sorts lexicographically by time,
// which makes log greps by request id naturally chronological.
func NewRequestID() string {
	id, err := uuid.NewV7()
	if err != nil {
		// NewV7 only fails if the system clock or rand source is broken;
		// fall back to v4 rather than panic the request path.
		return uuid.NewString()
	}
	return id.String()
}
