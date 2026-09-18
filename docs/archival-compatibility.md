# Archival protocol support

The RPC listener accepts CometBFT `/websocket` connections. Ordinary JSON-RPC
requests use the same height routing and historical fallback as HTTP, including
when sent on a socket with an active subscription. `subscribe`, `unsubscribe`
and `unsubscribe_all` use a selected tip backend and preserve request IDs.
The upstream WebSocket address is derived from the backend's RPC URL.

On upstream loss or a full message queue, Stitch closes the client with code
1013. Clients must reconnect and reconcile missed events; CometBFT subscriptions
do not provide replay. Ping/pong, disconnect cancellation and graceful shutdown
are supported. Each message is limited to 32 MiB, with queues bounded by both
message count and bytes. Subscription traffic is not cached.

## Browser and native gRPC on one hostname

The optional `grpc_web` listener shares the native gRPC server and its routing:

```yaml
listen:
  grpc: { addr: "0.0.0.0:5002" }
  grpc_web:
    addr: "0.0.0.0:5004"
    allowed_origins: ["https://app.example.com"]
```

An explicit `"*"` allows any browser origin; an empty list denies cross-origin
requests. The listener supports binary/text gRPC-Web, response status/trailers,
unary and server-streaming calls, and ordinary HTTP fallback to Cosmos REST.
Native `application/grpc` and `application/grpc+proto` requests use HTTP/2 h2c.
Native gRPC over HTTP/1 is rejected. The separate native port is unchanged.
The nonstandard gRPC-Web WebSocket transport is not enabled.

The listener requires `listen.grpc`. Configuration changes require a restart.
Native messages remain capped at 64 MiB; encoded web request bodies are capped
at 90 MiB. The ingress must allow configured CORS preflights while retaining
authorization on actual requests. Listener CORS does not replace ingress auth.

## Missing historical state

For an explicitly historical, idempotent request, known Cosmos store-retention
errors try another eligible backend within `policies.failover.max_attempts`.
The requested height, payload and metadata are preserved. Backend coverage is
not expanded and a historical query never silently becomes a latest query.

REST errors and CometBFT ABCI errors use a bounded structured-error inspection.
Other responses pass through unchanged. If all candidates lack the requested
state, the final upstream error is returned. Missing state does not count as a
shared circuit failure, and failed ABCI responses are not cached.

gRPC retries are limited to schema-verified unary read methods. Unknown methods,
broadcasts and streaming methods retain transparent forwarding. A response is
never retried after any message has been delivered. Headers and trailers from
failed attempts do not leak into a successful response; the final failure's
metadata is retained when all candidates fail. Transport/deadline failures still
affect backend health; unrelated application errors are returned unchanged.

The exact method inventory is generated from pinned Injective, Cosmos SDK, IBC
and CosmWasm schemas. Run `python3 tools/generate-history-methods.py` with an
authenticated `gh` CLI to regenerate it; source revisions and schema links are
recorded in `internal/server/cosmos_grpc/history_methods.go`.

## Asia migration

The archival Stitch release handles Cosmos/CometBFT chain traffic. EVM traffic
uses the existing Asia EVM Gateway, configured from height 127250000, without
archival IP/key authorization or rate tiers. Exchange endpoints remain on nginx.
Public routes and DNS are separate rollout steps.
