# stitch

> A height-aware multi-protocol gateway for Cosmos and Injective node fleets.

stitch sits in front of any number of upstream nodes — full archives, bounded
shards, pruned tips — and routes every request to the right one based on
height, hash, or method semantics. Backends fail without taking client
requests with them. Subscriptions resume across upstream restarts with
cursor-deduped continuity. Multiple clients on the same filter share one
upstream connection.

Open-source, MIT-licensed, written in Go.

```
                  ┌──────────────────┐
        client ──►│                  │──► full archive  (genesis..head)
        client ──►│                  │──► shard         (1..50M)
        client ──►│      stitch      │──► shard         (50M..head)
        client ──►│   (one process)  │──► pruned tip    (last 10k)
        stream ──►│                  │──► dr-archive    (cold standby)
        stream ──►│                  │──► ...
                  └──────────────────┘
                       ▲
                       │
                  admin API · Prometheus · structured logs
```

## Table of contents

- [Why stitch](#why-stitch)
- [Features](#features)
- [Quick start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [Concepts](#concepts)
- [Operations](#operations)
- [Performance](#performance)
- [Comparison to subnode and gateway](#comparison-to-subnode-and-gateway)
- [Examples](#examples)
- [Development](#development)
- [License](#license)

## Why stitch

Cosmos archive nodes get unwieldy fast — major chains add several TB per year,
and historical data is read-heavy but write-cold. Operators end up running:

- a small fleet of bounded "shards" each holding a slice of history
- a hot-tier pruned node for cheap latest-block reads
- a full archive as fallback for the long tail
- maybe a DR pair in another region

Doing this with a plain reverse proxy or round-robin LB doesn't work — you
need height-aware routing so a `block(height=12345)` request finds a shard
that actually has block 12345. You also want failover, circuit breaking,
subscription resume, hash-keyed memoization, and broadcast fan-out for tx
submission. **stitch is what you build instead of all of that.**

The two existing projects in this space — `decentrio/gateway` and
`InjectiveLabs/subnode` — each pick one or two of those concerns. stitch is
built from the ground up to do all of them at once, with a clean abstraction
where each piece is independently testable and replaceable.

## Features

- **8 protocol listeners**, all in one process:
  - CometBFT RPC (URI + JSON-RPC over HTTP)
  - Cosmos gRPC (transparent proxy + gRPC-Web)
  - Cosmos REST / gRPC-Gateway
  - EVM JSON-RPC HTTP (8 namespaces, 92 methods)
  - EVM JSON-RPC WebSocket
  - Injective ChainStream gRPC v1beta1 + v2
  - `/injstream-ws` JSON-RPC bridge
  - admin (`/healthz`, `/readyz`, `/metrics`, `/admin/*`)
- **Height-aware routing** with overlapping-range coverage shapes (archive,
  bounded, open, pruned). Selector picks narrowest backend that covers the
  request, sparing the archive for what actually needs it.
- **Failover** — circuit breakers per `(backend, protocol)`; transport,
  5xx, dial-error retries; non-idempotent broadcasts skip retry by design.
- **Subscription resume** — kill the upstream mid-stream and the client's
  WS or gRPC subscription survives. Cursor-deduped, monotonic delivery.
  Works for `eth_subscribe newHeads` + `logs`, `injective.stream.v*.Stream`,
  and `/injstream-ws`.
- **Multicast** — N clients with the same canonical filter share one
  upstream connection.
- **Hash → height memo** — `block_by_hash`, `eth_getBlockByHash`,
  `eth_getTransactionByHash`, etc. go from O(N backends) to O(1) lookups.
- **Broadcast fan-out** — tx submission is sent to all healthy backends
  in parallel; first ack wins. No silent drops on partitioned backends.
- **Hedging** — slow primary triggers a parallel attempt on the second
  candidate; whoever responds first wins.
- **Response cache** — finalized historical reads (height ≤ head − confirmation_depth)
  cache for the configured TTL. Idempotent hash-keyed reads cache forever.
- **Hot reload** — `SIGHUP` or `POST /admin/reload` re-reads the config
  and atomically swaps the backend registry.
- **Drain control** — `POST /admin/backends/<name>/drain` removes a
  backend from rotation without dropping in-flight work.
- **Observability** — Prometheus metrics for every dimension,
  structured slog with UUIDv7 request IDs, `${VAR}` env expansion in
  config files.
- **Open source** — MIT, no auth tiers, no quotas, no API keys. If you
  need rate limits or auth, front stitch with your own proxy.

## Quick start

```bash
git clone https://github.com/decentrio/stitch
cd stitch
make build

# Generate a starter config
./build/stitch init -o config.yaml

# Edit config.yaml to point at your upstreams, then run
./build/stitch start --config config.yaml
```

In another terminal:

```bash
curl -s http://127.0.0.1:9091/healthz
curl -s http://127.0.0.1:9091/admin/backends | jq
curl -s http://127.0.0.1:9091/metrics | grep stitch_
```

## Installation

### From source

Requires Go 1.21+.

```bash
git clone https://github.com/decentrio/stitch
cd stitch
make build         # produces ./build/stitch
make install       # or: go install into $GOPATH/bin
```

### Docker

```bash
docker build -t stitch:latest .
docker run -d \
  --name stitch \
  -p 5001-5008:5001-5008 \
  -p 9091:9091 \
  -v $PWD/config.yaml:/etc/stitch/config.yaml \
  --env-file .env \
  stitch:latest start --config /etc/stitch/config.yaml
```

## Configuration

The config is YAML. All keys use `snake_case`. Unknown keys are rejected at
load time so typos surface immediately.

### Environment variable expansion

`${VAR_NAME}` and `$VAR_NAME` tokens in the YAML are replaced with the value
of process environment variables before parsing. Useful for secrets and
per-environment URLs:

```yaml
backends:
  - name: archive
    endpoints:
      rpc: ${ARCHIVE_RPC_URL}
```

See [`.env.example`](.env.example) for a full env file template.

### Listener block

Eight optional listeners. An empty `addr` disables the listener.

```yaml
listen:
  rpc:         { addr: "0.0.0.0:5001" }   # CometBFT URI + JSON-RPC
  grpc:        { addr: "0.0.0.0:5002" }   # Cosmos gRPC (+ gRPC-Web)
  api:         { addr: "0.0.0.0:5003" }   # Cosmos REST
  eth_rpc:     { addr: "0.0.0.0:5005" }   # EVM JSON-RPC HTTP
  eth_ws:      { addr: "0.0.0.0:5006" }   # EVM JSON-RPC WS
  chainstream: { addr: "0.0.0.0:5007" }   # injective.stream.v*
  inj_ws:      { addr: "0.0.0.0:5008" }   # /injstream-ws
  admin:       { addr: "127.0.0.1:9091" } # bind to loopback by default
```

### Backends

Each backend declares its **coverage** — what range of blocks it serves —
and a map of **endpoints** per protocol.

```yaml
backends:
  - name: archive
    coverage: { kind: archive }
    weight: 100
    tags: [archive, primary]
    endpoints:
      rpc:     http://node:26657
      grpc:    node:9900
      api:     http://node:1317
      eth_rpc: http://node:8545
      eth_ws:  ws://node:8546
```

The four **coverage** shapes:

| Kind       | Shape                                       | Example                         |
|------------|---------------------------------------------|---------------------------------|
| `archive`  | genesis..head                               | `{ kind: archive }`             |
| `bounded`  | closed interval `[lower, upper]`            | `{ kind: bounded, lower: 1, upper: 50000000 }` |
| `open`     | half-open `[lower, head)`                   | `{ kind: open, lower: 50000001 }` |
| `pruned`   | sliding window of last `keep` blocks        | `{ kind: pruned, keep: 100000 }` |

The selector ranks eligible backends by **specificity** (narrower first),
**health**, and **circuit state**. Within ties, higher `weight` wins. Use
weight to bias a primary region over a DR region; use coverage shape to
spare the archive for cold reads.

### Policies

```yaml
policies:
  failover:
    max_attempts: 3                    # how many candidates to try
    per_attempt_timeout: 5s            # per-upstream deadline
  hedging:
    enabled: true                      # parallel attempt on slow primary
    methods: [eth_call, abci_query]    # which methods qualify
  circuit:
    error_threshold: 0.5               # 50% failure rate trips
    min_requests: 20                   # over a minimum window of 20 calls
    open_duration: 30s                 # before half-open canary
  cache:
    enabled: true
    confirmation_depth: 100            # head − N is the cacheable boundary
    l1_size_mb: 1024                   # in-process LRU byte budget
  health:
    probe_interval: 5s                 # how often to probe each backend
    max_lag_blocks: 50                 # trip out backends lagging head by N
  subscriptions:
    multicast: true                    # N clients / 1 upstream
    slow_consumer: drop                # drop | disconnect | backpressure
    replay_timeout: 30s                # max wait for resume cursor catch-up
  dangerous_methods:
    allow:                             # opt-in for debug_*/personal_*/miner_*
      - debug_traceCall
      - debug_traceTransaction
```

### Logging

```yaml
log:
  level: info     # debug | info | warn | error
  format: json    # json | text
```

## Concepts

### Coverage and routing

A request like `block?height=12345` hits stitch's CometBFT RPC listener.
The decoder extracts height = 12345. The selector iterates backends:

- `archive` — eligible (1 ≤ 12345 ≤ head), specificity score low
- `shard-1` (bounded 1..50M) — eligible, specificity score high
- `shard-2` (open 50M+) — not eligible (12345 < 50M)
- `hot-tip` (pruned 100k) — not eligible (12345 < head − 100k)

Selector ranks: shard-1 first (highest specificity), then archive. The
forwarder tries shard-1; on success, return. On 5xx or transport error,
tries archive; circuit-records the shard-1 failure. After enough failures
the shard-1 circuit opens and selector skips it entirely until cooldown.

### Subscription resume

When a client opens an `eth_subscribe newHeads`, stitch:

1. Mints a synthetic subscription ID and returns it to the client.
2. Opens an upstream subscription via the same protocol.
3. Forwards each notification with the synthetic ID rewritten in.
4. Tracks a per-client cursor (block number, log index, etc.).

If the upstream dies, stitch:

1. Detects the read error.
2. Re-dials the next eligible backend.
3. Re-issues the subscribe with the original filter.
4. Drops every event whose cursor ≤ last delivered (no duplicates).
5. Resumes forwarding to the client with the same synthetic ID.

The client sees a continuous stream — same ID, monotonic cursor, no gap.

### Multicast (`/injstream-ws`)

When 100 clients subscribe to the same filter, stitch canonicalizes the
filter (sorted keys, sorted string arrays), hashes it, and uses the hash
as a multicast key. The first client opens an upstream subscription; the
remaining 99 attach to the same upstream. One TCP connection, fan-out
to N clients.

### Hash → height memo

`eth_getBlockByHash(0xabc)` doesn't tell stitch which backend has the
block. Without help, stitch would have to try multiple. Instead, stitch
maintains an LRU `hash → height` index, populated by:

- successful responses to height-keyed reads (`eth_getBlockByNumber`, etc.)
- successful responses to hash-keyed reads (`eth_getTransactionByHash`, etc.)

On a cache hit, the hash route becomes a height route — O(1) routing
decision against the right shard.

### Response cache

For finalized reads (height ≤ head − confirmation_depth), stitch caches
the entire response body keyed by `(protocol, method, height, params hash)`.
The next identical call serves from local memory in ~350 ns with no
upstream traffic.

## Operations

### Admin API

The admin listener serves these endpoints (default `127.0.0.1:9091`):

| Method | Endpoint                                   | Effect                                |
|--------|--------------------------------------------|---------------------------------------|
| GET    | `/healthz`                                 | always 200 if listening               |
| GET    | `/readyz`                                  | 200 once cross-cutting init complete  |
| GET    | `/metrics`                                 | Prometheus exposition                 |
| GET    | `/admin/backends`                          | list all backends + health + circuit  |
| GET    | `/admin/backends/{name}`                   | per-backend detail                    |
| POST   | `/admin/backends/{name}/drain`             | remove from selector (in-flight ok)   |
| POST   | `/admin/backends/{name}/enable`            | undrain                               |
| GET    | `/admin/cache/stats`                       | hash-index + response-cache sizes     |
| POST   | `/admin/reload`                            | re-read config; same as SIGHUP        |

Example session:

```bash
# Drain a backend before maintenance
curl -X POST http://localhost:9091/admin/backends/archive-1/drain

# Maintenance happens; in-flight requests finish on archive-1, new
# ones route to other backends.

# Bring it back
curl -X POST http://localhost:9091/admin/backends/archive-1/enable
```

### Hot reload

Edit `config.yaml`, then either:

```bash
kill -HUP $(pgrep stitch)
# or
curl -X POST http://localhost:9091/admin/reload
```

The new backend list takes effect for the next request. In-flight requests
keep their captured snapshot. Drain state persists across reloads — operators
don't expect "drained" to silently flip back when the file changes.

### Metrics

stitch exposes ~15 Prometheus metric families. Hot ones:

```
stitch_requests_total{protocol,method_class,backend,status}
stitch_request_duration_seconds{protocol,method_class,backend}
stitch_backend_health{backend,protocol}                    # 0|1
stitch_backend_lag_blocks{backend}
stitch_circuit_state{backend,protocol}                     # 0=closed,1=half,2=open
stitch_failover_attempts_total{from,to,reason}
stitch_cache_total{layer,result}                           # hashidx|response, hit|miss|evict|expired
stitch_subscriptions_active{protocol,kind}
stitch_subscription_resumes_total{reason}
stitch_broadcast_fanout_total{result}                      # success|partial|total_failure|all_circuited
stitch_hedge_wins_total{method,winner_index}               # 0|1
```

### Logging

JSON or text via `log.format`. Every line carries `request_id` (UUIDv7 — sortable),
`protocol`, `method`, `backend` where relevant. Use `request_id` to correlate
client request → routing decision → upstream call → response.

## Performance

Measured on M-series Mac (`go test -bench=.`):

| Operation                              | Latency  | Design target           |
|----------------------------------------|---------:|-------------------------|
| Hash → height index hit                | 204 ns   | < 1 µs                  |
| Response cache hit                     | 356 ns   | P50 < 1 ms              |
| Cache key build                        | 71 ns    | —                       |
| Selector decision (32 backends)        | 780 ns   | —                       |
| Full forward end-to-end (mock)         | 45.8 µs  | P50 < 5 ms              |

A "full forward" measures stitch's added overhead in isolation:
selector → circuit → connection pool → HTTP RTT → header copy → body
relay. Against real upstreams, total latency is dominated by the upstream
node's response time; stitch's contribution stays in the tens of µs.

Validated under chaos:
- 5 mock backends with rolling kills every 50 ms (≥2 always alive)
- 500 concurrent client requests
- **100 % success rate**

## Comparison to subnode and gateway

|                                  | InjectiveLabs/subnode | decentrio/gateway | **stitch** |
|----------------------------------|:-------------------:|:----------------:|:----------:|
| Height-aware routing             | ✓                   | ✓                | ✓          |
| Overlapping-range coverage       | partial             | partial          | first-class |
| Failover                         | ✗                   | ✗                | ✓          |
| Circuit breakers                 | ✗                   | ✗                | ✓          |
| Subscription resume              | ✗                   | ✗                | **✓**      |
| Multicast (1 upstream / N clients)| ✗                  | ✗                | ✓          |
| Broadcast fan-out                | ✗                   | ✗                | ✓          |
| Hedging                          | ✗                   | ✗                | ✓          |
| Hash → height memo               | ✗                   | ✗                | ✓          |
| Response cache                   | ✗                   | partial          | ✓          |
| Hot config reload                | ✗                   | ✗                | ✓          |
| Drain control                    | ✗                   | ✗                | ✓          |
| Prometheus metrics               | ✗                   | ✗                | ✓          |
| Structured logs                  | ✗                   | ✗                | ✓          |

## Examples

Ready-made configs in [`examples/`](examples/):

- [`local-dev.yaml`](examples/local-dev.yaml) — single localhost upstream, RPC + admin only
- [`injective-archive.yaml`](examples/injective-archive.yaml) — single archive, all listeners on
- [`sharded.yaml`](examples/sharded.yaml) — bounded shards + open + pruned + archive fallback
- [`multi-region.yaml`](examples/multi-region.yaml) — primary + DR with weighted preference

## Development

### Build & test

```bash
make build              # ./build/stitch
make test               # full suite (~25s)
make test-race          # full suite with -race (~40s)
make lint               # golangci-lint
```

### Layout

```
cmd/stitch                  entrypoint
internal/
├── cmd                     cobra subcommands (root/start/init/version/migrate)
├── config                  typed config + YAML loader + validator + atomic holder
├── log                     slog wrapper (forbidden in favor of fmt.Print* via golangci)
├── metrics                 Prometheus collectors
├── tracing                 no-op Span API (OTLP swap-in stays compatible)
├── runtime                 UUIDv7 request IDs
├── admin                   /healthz /readyz /metrics /admin/{backends,cache,reload}
├── server
│   ├── lifecycle           errgroup + signal handling for protocol listeners
│   ├── cmt_rpc             CometBFT RPC (URI + JSON-RPC over HTTP)
│   ├── cosmos_grpc         Cosmos gRPC transparent proxy + outcome recording
│   ├── cosmos_rest         Cosmos REST / gRPC-Gateway proxy
│   ├── eth_rpc             EVM JSON-RPC HTTP — 92 methods across 8 namespaces
│   ├── eth_ws              EVM JSON-RPC WS — subscription session + resume
│   ├── chainstream         injective.stream.v1beta1 + v2 with cursor-resume
│   └── inj_ws              /injstream-ws JSON-RPC bridge with resume
├── types                   Protocol, MethodClass, RouteKey
├── backend                 Backend, Coverage, Registry (drain-aware)
├── health                  Per-backend health snapshots + RPC/REST/gRPC probes
├── circuit                 Per-(backend,protocol) circuit breakers
├── pool                    HTTP transport pool + gRPC ClientConn pool with eviction
├── selector                Range-based candidate scoring (specificity + health + lag)
├── forwarder               HTTP forwarder with retry, broadcast fan-out, hedging
├── subscription            Cursor + session + cross-protocol resume + multicast hub
└── cache                   Hash → height memo + response cache + key + policy

manifests                   per-protocol method manifests (ship embedded)
examples                    ready-made config files
test/integration            cross-package smoke + chaos + leak tests
```

### Contributing

1. `make test-race` must stay green.
2. Add a manifest entry when you add a method (CometBFT RPC, EVM JSON-RPC, or Cosmos gRPC). Tests cross-check the manifest against the [Injective query reference](https://github.com/InjectiveFoundation/injective-core).
3. New listeners follow the `Server` interface contract from `internal/server/lifecycle.go`: `Name()`, `Start(ctx)`, `Shutdown(ctx)`. Eager listener bind in `New()` for race-free `Addr()`.
4. Avoid `fmt.Print*` and the stdlib `log` package — `golangci-lint`'s `forbidigo` enforces this. Use `internal/log`.

## License

MIT.
