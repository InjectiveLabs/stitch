# Changelog

All notable changes to stitch are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Graceful shutdown returns as soon as every listener has drained instead of
  always blocking for the full `--shutdown-grace` window; per-server drain
  timings are logged, and servers still draining at the deadline are named.
- A listener failing to start (e.g. a port conflict) now drains its peers
  instead of leaving them running with signal handling disabled — previously
  the process could only be stopped with SIGKILL.

### Changed

- Bounded-coverage backends are verified once at startup (with retry) instead
  of being probed every `probe_interval`; their coverage is static, so
  periodic `/status` polls were wasted work.
- CI runs on pushes to `master` (the actual default branch) and tests
  Go 1.25 + stable, matching the `go.mod` directive. README now states the
  real minimum (Go 1.25+).

## [0.1.0] — initial release

First public cut. Stitch fronts an arbitrary number of upstream Cosmos /
Injective nodes (archives, bounded shards, pruned tips) and routes
requests to the right one based on height, hash, or method.

### Listeners

- CometBFT RPC (URI + JSON-RPC over HTTP)
- Cosmos gRPC transparent proxy
- Cosmos REST / gRPC-Gateway
- EVM JSON-RPC HTTP — 8 namespaces, 92 methods
- EVM JSON-RPC WebSocket
- Injective ChainStream gRPC (v1beta1 + v2)
- `/injstream-ws` JSON-RPC bridge
- Admin API (`/healthz`, `/readyz`, `/metrics`, `/admin/*`)

### Routing

- Four coverage shapes: `archive`, `bounded`, `open`, `pruned`
- Specificity-weighted selector (narrower coverage wins ties)
- Per-(backend, protocol) circuit breakers
- Active health probing (RPC `/status`, REST `latest`, gRPC reachability)
- Lag-aware exclusion via `max_lag_blocks`

### Reliability

- Failover across selector candidates on transport / 5xx errors
- Broadcast fan-out for tx submission (`broadcast_tx_*`, `eth_sendRawTransaction`)
- Hedging for slow idempotent reads (`eth_call`, `abci_query`, …)
- Subscription resume with cursor dedup across 4 protocols:
  `eth_subscribe newHeads`, `eth_subscribe logs`, `injective.stream.v*`,
  `/injstream-ws`
- Multicast hub for `/injstream-ws` — N clients with the same filter
  share one upstream connection (slow-consumer policy: drop / disconnect / backpressure)

### Caching

- Hash → height memo (LRU, 100k entries) — routes `eth_getBlockByHash`,
  `block_by_hash`, `tx`, `eth_getTransactionByHash`, etc. via O(1) lookup
- Response cache (LRU, byte-budgeted) — finalized historical reads
  served from local memory
- Confirmation-depth gating (configurable, default 100 blocks)

### Operations

- Drain control via `POST /admin/backends/<name>/drain` — in-flight
  requests finish, new ones route around
- Hot reload via `SIGHUP` or `POST /admin/reload`
- `${VAR}` environment-variable expansion in config files
- Dangerous methods (`debug_*`, `personal_*`, `miner_*`) default-denied;
  per-method opt-in via `policies.dangerous_methods.allow`

### Observability

- 15 Prometheus metric families covering requests, latencies, health,
  circuit state, cache hit rates, broadcast outcomes, hedge wins,
  subscription resumes
- Structured slog with UUIDv7 request IDs

### Bundled tooling

- `stitch init` — write a starter config
- `stitch version` — build info
- `stitch start --config <path>` — boot the listener stack
- 4 example configs (local-dev, single-archive, sharded, multi-region)
- Dockerfile (distroless, nonroot, ~30 MB) + docker-compose example

### Known limitations

- chainstream gRPC + eth_ws multicast not yet implemented; only
  `/injstream-ws` benefits from the hub today. Same `Hub` building
  block, just needs the per-protocol adapter (see plan §47).
- Synthetic gap signaling when upstream cursor advance can't preserve
  continuity is planned but not yet emitted.
- No L2 cache (shared Redis). Each replica's L1 is independent.

[Unreleased]: https://github.com/decentrio/stitch/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/decentrio/stitch/releases/tag/v0.1.0
