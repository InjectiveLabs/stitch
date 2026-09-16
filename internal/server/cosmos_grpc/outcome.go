package cosmos_grpc

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rpcOutcome string

const (
	rpcSuccess rpcOutcome = "success"
	rpcFailure rpcOutcome = "failure"
	rpcNeutral rpcOutcome = "neutral"
)

// classifyRPCOutcome selects evidence for the shared backend/protocol
// breaker, without changing the status returned to the caller. Only explicit
// availability and deadline failures count against it. Internal and Unknown
// are ambiguous: upstreams also use them for deterministic application errors,
// so treating them as outages can block unrelated methods on a healthy node.
func classifyRPCOutcome(ctx context.Context, err error) rpcOutcome {
	if err == nil {
		return rpcSuccess
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		deadline, hasDeadline := ctx.Deadline()
		if !hasDeadline || time.Now().Before(deadline) {
			// A future deadline is not evidence that it expired. Prefer the
			// caller's early cancellation even if teardown returns Unavailable.
			return rpcNeutral
		}
		// grpc-go can report client deadline expiry as server-side Canceled.
		// Count it only once the supplied deadline has actually elapsed.
		return rpcFailure
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return rpcFailure
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return rpcFailure
	default:
		// Release, rather than success, leaves a half-open circuit probing
		// while freeing its admission for a later healthy request.
		return rpcNeutral
	}
}
