# Changelog

All notable changes to stitch are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `/injstream-ws` multicast is now actually wired, behind
  `policies.subscriptions.multicast` (default **false**). It was advertised
  in the README and parsed from config, but the multicast hub had zero
  production callers — every client always got its own upstream connection.
  With the flag on, clients whose filters share a canonical key share one
  upstream; the shared subscription resumes across backend failure with
  cursor dedup. **In multicast mode, non-`subscribe`/`unsubscribe` JSON-RPC
  frames on `/injstream-ws` are rejected with a `-32601` error instead of
  per-session passthrough** (there is no per-client upstream to forward
  them to). Subscribe acks are synthesized at attach time; if the upstream
  rejects the shared subscribe (JSON-RPC error), the hub tears that
  upstream down and the attached clients are closed with WS 1013 — the
  rejected filter is never replayed — counted in
  `stitch_subscription_dropped_notifications_total{reason="upstream_reject"}`.
- The rest of `policies.subscriptions` is now real (previously parsed but
  consumed by nothing): `slow_consumer` governs the multicast fan-out
  policy per client (`drop` | `disconnect` | `backpressure`; drops are
  counted in `stitch_subscription_dropped_notifications_total{reason=
  "slow_consumer"}`), the new `send_buffer` knob sizes the per-client
  fan-out queue (default 64), and `replay_timeout` is the max time a
  resume keeps re-dialing for an upstream (250ms→2s backoff between
  passes) before the session/subscriber is dropped — omitted it defaults
  to 30s, while an explicit `0` keeps the previous single-pass behavior
  (one dial pass per resume); negative values are rejected at load.
- `policies.cache.ttl`, `policies.cache.hash_index_entries`, and
  `policies.cache.response_entries` are now wired: they size the hash→height
  index and the response cache and set the response-entry lifetime
  (previously parsed but ignored). `policies.hedging.hedge_after` likewise
  controls the hedge delay for real.
- Hot reload now warns about edited config sections that only apply at
  startup (`listen`, `policies.*`), diffed against the **boot** config so
  the warning repeats on every reload while the file diverges and stays
  silent when it reverts.
- Hot reload prunes health snapshots, circuit breakers, and their
  per-backend metric gauge children for backends the new config no longer
  declares; in-flight probes and the eth_ws head tracker can no longer
  resurrect a pruned backend. Cumulative counters (`stitch_requests_total`
  etc.) are deliberately retained as history.
- `POST /admin/cache/purge` actually purges both caches and reports per-cache
  purged-entry counts (was a stub that purged nothing); purges are counted in
  `stitch_cache_total{result="purge"}`.

### Removed

- `policies.hedging.after_pct_of_p95` — the knob was never wired to
  anything. Config parsing is strict, so configs still setting it will now
  fail to load; delete the line (use `hedge_after` for the hedge delay).

### Fixed

- WS subscription upstreams (eth_ws and `/injstream-ws` sessions, and the
  multicast hub) now send keepalive pings every 20s; previously a quiet
  stream hit the 60s read deadline and silently churned through a resume
  — and under multicast that hiccup would have hit every attached client
  at once.
- EVM-only backends (no CometBFT `rpc` endpoint) are now health-gated via
  their `eth_ws` head stream; a dropped stream marks the backend unhealthy
  for EVM/stream routing until it reconnects. Previously such backends were
  optimistically healthy forever.
- Graceful shutdown returns as soon as every listener has drained instead of
  always blocking for the full `--shutdown-grace` window; per-server drain
  timings are logged, and servers still draining at the deadline are named.
- A listener failing to start (e.g. a port conflict) now drains its peers
  instead of leaving them running with signal handling disabled — previously
  the process could only be stopped with SIGKILL.

### Changed

- **BEHAVIOR**: hedging is now genuinely config-gated. Configs without
  `policies.hedging.enabled: true` no longer hedge at all; previously the
  manifest's hedge-safe flag alone enabled hedging on the EVM JSON-RPC
  listener regardless of config. Hedging still applies to the eth_rpc
  listener only.
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
