// Package tracing exposes a minimal Span API. Phase 0 ships a no-op
// implementation; phase 9 wires OTLP without changing call sites.
package tracing

import "context"

// Span is a small, OTel-compatible interface. Callers always defer End().
type Span interface {
	End()
	SetError(err error)
	SetAttribute(key string, value any)
}

type noopSpan struct{}

func (noopSpan) End()                       {}
func (noopSpan) SetError(error)             {}
func (noopSpan) SetAttribute(string, any)   {}

// Start returns ctx unchanged plus a no-op span. Replace the implementation
// behind this function to enable real tracing.
func Start(ctx context.Context, _ string) (context.Context, Span) {
	return ctx, noopSpan{}
}
