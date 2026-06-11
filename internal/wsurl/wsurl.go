// Package wsurl normalizes operator-configured endpoint URLs into
// dialable WebSocket URLs. It exists to be the single copy of the
// scheme-mapping logic that previously lived (four times, with subtly
// different prefix matching) in the subscription sessions, the
// subscription hub, the eth_ws health prober, and the eth_ws listener.
//
// The package deliberately has no dependencies inside this repo so any
// package — health, subscription, servers — can import it without cycles.
//
// Matching is case-sensitive and recognizes only complete schemes
// ("ws://", not "ws:/"); malformed near-scheme strings fall through to
// the default branch rather than being sliced apart byte-wise like some
// of the historical copies did.
package wsurl

import "strings"

// Normalize maps an http(s):// endpoint to its ws(s):// equivalent.
// Already-ws(s) URLs and anything without a recognized scheme (bare
// host:port, unknown schemes) pass through unchanged.
func Normalize(ep string) string {
	switch {
	case strings.HasPrefix(ep, "ws://"), strings.HasPrefix(ep, "wss://"):
		return ep
	case strings.HasPrefix(ep, "http://"):
		return "ws://" + strings.TrimPrefix(ep, "http://")
	case strings.HasPrefix(ep, "https://"):
		return "wss://" + strings.TrimPrefix(ep, "https://")
	default:
		return ep
	}
}

// InjStreamURL maps an operator-provided endpoint to the /injstream-ws
// dial URL. ChainStream gRPC endpoints are configured as bare host:port;
// /injstream-ws is HTTP+WS so a ws:// or wss:// scheme is required:
//
//   - ws:// or wss:// → used as-is (operator supplied the full URL,
//     including any path)
//   - http:// or https:// → scheme swapped to ws/wss, /injstream-ws
//     appended
//   - anything else (bare host:port) → assume insecure ws, append
//     /injstream-ws
func InjStreamURL(ep string) string {
	switch {
	case strings.HasPrefix(ep, "ws://"), strings.HasPrefix(ep, "wss://"):
		return ep
	case strings.HasPrefix(ep, "http://"):
		return "ws://" + strings.TrimPrefix(ep, "http://") + "/injstream-ws"
	case strings.HasPrefix(ep, "https://"):
		return "wss://" + strings.TrimPrefix(ep, "https://") + "/injstream-ws"
	default:
		return "ws://" + ep + "/injstream-ws"
	}
}
