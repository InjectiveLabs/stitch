// Package types holds enumerations and small value types shared across the
// router/selector/forwarder/server packages.
package types

// Protocol identifies one of the listener-facing protocols stitch fronts.
type Protocol string

const (
	ProtoRPC         Protocol = "rpc"         // CometBFT JSON-RPC + URI
	ProtoGRPC        Protocol = "grpc"        // Cosmos gRPC
	ProtoAPI         Protocol = "api"         // Cosmos REST (gRPC-Gateway)
	ProtoEthRPC      Protocol = "eth_rpc"     // EVM JSON-RPC over HTTP
	ProtoEthWS       Protocol = "eth_ws"      // EVM JSON-RPC over WebSocket
	ProtoChainStream Protocol = "chainstream" // injective.stream.v* gRPC
)

// MethodClass classifies how a method is routed and whether failover/retry
// applies. The forwarder reads it; the per-protocol decoder fills it in.
type MethodClass int

const (
	// ClassStateless: no upstream call needed (e.g. eth_chainId backed by
	// config). Forwarder may still proxy if no local handler is registered.
	ClassStateless MethodClass = iota

	// ClassLatest: routed to any backend whose head we trust.
	ClassLatest

	// ClassByHeight: route by RouteKey.Height.
	ClassByHeight

	// ClassByHeightRange: route to a backend that covers every height in
	// RouteKey.Range.
	ClassByHeightRange

	// ClassByHash: hash → height memo (phase 6); fan-out to candidates on miss.
	ClassByHash

	// ClassSubscribe: stream subscription; managed by the subscription hub.
	ClassSubscribe

	// ClassBroadcast: tx submission; fan out to all healthy backends.
	ClassBroadcast
)

func (c MethodClass) String() string {
	switch c {
	case ClassStateless:
		return "stateless"
	case ClassLatest:
		return "latest"
	case ClassByHeight:
		return "by_height"
	case ClassByHeightRange:
		return "by_height_range"
	case ClassByHash:
		return "by_hash"
	case ClassSubscribe:
		return "subscribe"
	case ClassBroadcast:
		return "broadcast"
	default:
		return "unknown"
	}
}
