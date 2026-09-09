# Earliest EVM history through Stitch

Stitch discovers the first retained, initialized EVM state across the active
archive shards. Configured Cosmos coverage bounds the search; it does not prove
that EVM state exists at the lower bound. There is no hardcoded activation height.

## Gateway contract

With `WEB3INJ_STITCH_BACKEND=true`, evm-gateway resolves an explicit `earliest`
selector before constructing dependent reads. It calls
`/injective.evm.v1.Query/Params` with an empty protobuf request and metadata:

```text
stitch: earliest
x-stitch-earliest-capability: state
x-cosmos-block-height: 1
```

The last header is a legacy rollout placeholder. Upgraded Stitch consumes the
marker, discards the placeholder, and probes concrete heights. Old Stitch does
not produce the required discovery response headers, so the gateway rejects its
response rather than treating it as successful discovery.

A successful response contains the Params protobuf and three required headers:

```text
x-stitch-earliest-height: <resolved Cosmos height>
x-cosmos-block-height: <same resolved Cosmos height>
x-stitch-backend: <verified backend name>
```

The gateway pins dependent gRPC and Comet reads to that height and backend using
`x-stitch-backend`. A hint is accepted only for concrete historical reads and
does not bypass endpoint, coverage, drain, health, or circuit checks. If the
verified backend becomes unavailable, the operation fails instead of silently
using an unverified snapshot. Pinned Comet requests bypass Stitch's response
cache so a cached response cannot bypass the selected backend's current
eligibility checks. Internal routing headers are consumed before requests reach
a normal upstream node.

## Capabilities

`x-stitch-earliest-capability` accepts one value, defaulting to `state`:

| Value | Required data at candidate height H |
| --- | --- |
| `state` | Initialized EVM Params and retained state at H |
| `execution` | State and Comet block at H |
| `block` | State, Comet block, and block results at H |
| `range` | Same as block; resolves the boundary, not the complete result range |
| `trace` | State at H and H−1, block and block results at H |
| `proof` | State, block, and EVM/auth store proofs at H; H must exceed 2 |
| `storage` | State and EVM store subspace access at H |

Range execution remains responsible for every block in the requested interval;
discovering a starting height does not establish later shard coverage or permit
returning partial logs. Executions and proofs use H, matching the actual gateway
and SDK request paths. Only block tracing requires the parent snapshot H−1.

Read-only unary Account, CosmosAccount, Balance, Code, Storage, BaseFee, and
Params queries can also use the marker directly. Stitch buffers their responses
until it selects the winner. Other methods must resolve Params first, then
construct their concrete-height request. Streaming and write methods reject the
marker.

## Discovery and validity

Each eligible archive, bounded, or open shard first reports its observed head
through Params. Stitch intersects that height with configured coverage, checks
the lower bound, and binary-searches the first available EVM state when necessary.
The first stage uses only the small Params response. Required block, results,
proof, or operation-specific state are checked at that resolved floor; another
binary search is needed only when that capability begins later. This avoids
fetching large arbitrary midpoint blocks during ordinary state discovery.
Tracing starts that second stage one block after the established state floor,
because its parent must also have retained initialized EVM state.
This search assumes each shard retains a contiguous historical state window and
that EVM initialization is persistent once established. It does not assume that
an account's existence, transaction contents, or application results are
monotonic. Disjoint state retention must be represented as separate coverage
windows; the resolver does not scan arbitrary holes block by block.

A valid Params response requires a nonempty `params.evm_denom` and an exact
`x-cosmos-block-height` response. Successful pre-EVM default Params therefore do
not qualify. Recognized SDK missing-store-version errors mean the snapshot is
unavailable. Timeouts, malformed responses, wrong heights, and unexpected errors
leave discovery unresolved. Capability probes also verify their returned heights
and required block/results/proof presence. A missing capability permits other
shards to answer.

For direct state queries, successful zero or empty payloads and a normal
account-NotFound response are valid answers at the resolved snapshot. They never
cause a search for a later nonempty account. Missing historical state is a
separate outcome and never becomes a zero nonce or empty success. Hash/object
lookups do not use this state-discovery adapter.

All candidate outcomes are compared by verified height; completion order does
not matter. Equal heights use configured weight and then backend name. A failed
candidate that could contain an older answer causes `Unavailable: earliest
discovery incomplete`. A working replica at the same known lower bound can
establish the same oldest height even if another replica fails. A replica whose
actual retained floor is later cannot prove what an unavailable older replica
contains, so that case remains incomplete. Explicitly drained backends and
pruned live nodes are outside archive discovery scope. Health- or circuit-blocked
archive shards remain evidence of an incomplete search when potentially older.

If all reachable candidates prove that the requested history/capability is
unavailable, Stitch returns `FailedPrecondition`, distinct from an application's
`NotFound` response. No newer result is labeled as earliest while older evidence
is unresolved.

## Resource bounds and rollout

Discovery has a 30-second overall budget, bounded by any shorter caller
deadline. Unary gRPC probes have a 3-second budget; Comet reads allow 10 seconds
for large activation-block results, still within the overall budget. Four workers search each
request's shards, with at most 32 simultaneous probes across a Director and a
256-candidate per-request limit. Historical search is logarithmic in each
configured window. Request payloads are capped at 1 MiB, Params responses at
64 KiB, and selected unary query responses at 8 MiB. Only the winning query
payload is retained after each candidate completes. Comet capability responses
have a 128 MiB read budget but stream through a bounded JSON projection: large
transaction, event, and proof strings are validated and discarded instead of
being buffered. Every proof operation must be an object with nonempty string
`type` and `data` fields; the data is discarded after validating its JSON shape.
This witnesses proof availability without claiming cryptographic verification.
Retained metadata is limited to 64 KiB with a maximum nesting
depth of 64. Truncated, malformed, trailing, and over-budget JSON fail discovery.

Discovery runs per operation and does not cache a floor that could become stale
after an archive restore. This adds historical query load and latency; enable
the gateway flag only after deploying Stitch and validating archive state and
capability coverage. Selected and incomplete outcomes appear in
`stitch_requests_total` with `method_class="earliest"`.

Tracked by ID-1573, a sub-issue of ID-1568. Independent tests exercise the actual
gRPC and Comet listeners with historical fixtures, response ordering, health,
circuits, exact metadata, empty state, affinity, and request budgets.
