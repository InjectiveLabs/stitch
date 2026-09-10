# Archive parity plan

Tracked by ID-1568. Stitch: ID-1573 / PR #5. Gateway: ID-1574 / PR #14.

## Contract

Splitting the same chain history across backends must not change the meaning
or result of a historical request. Example shard boundaries from discussion
are illustrative, not deployment facts or activation constants.

Use one explicit, operator-verified logical EVM archive start E and Cosmos
chain identity. Configure Stitch's `archive.evm_start_height` and
`archive.cosmos_chain_id`; configure the same `WEB3INJ_ARCHIVE_START_HEIGHT`
in both the direct-full and Stitch-enabled gateway. This is separate from
the indexer's backfill setting. No profile is inferred from healthy shards,
initialized Params, an account's existence, or an unrelated upgrade height.
An unspecified profile leaves ordinary legacy routing unchanged; enabling
Stitch integration requires an explicit profile. Production remains opt-in.

Every method maps `earliest` to E. Capability affects whether a request can
be served at E, never which block `earliest` means. A trace needs its actual
parent-state dependency; unavailable parent state is an error, not E+1.
Zero, empty code/storage, and an absent account are valid snapshot outcomes.
They do not trigger a search for the account's first appearance.

## Implementation

1. Replace capability-floor discovery with the fixed archive contract.
   Preserve a versioned metadata handshake and reject mismatched profiles.
   Resolve cached immutable reads without depending on a live per-call probe.
2. Buffer supported historical unary gRPC and Comet replies until validated.
   Retry unavailable, pruned, malformed, or wrong-height responses against
   eligible replicas at exactly the same height. Validate chain identity.
   Do not tie all dependent reads to one physical backend.
3. Search unknown block hashes and the gateway's exact Ethereum hash event
   query across historical shards with bounded work. Validate positive
   identity, deduplicate replicas, and distinguish local misses from complete
   global absence. Declared coverage remains visible during drain/outage.
   Event indexes cannot prove absence of failed EVM transactions: require a
   complete gateway index or report incomplete lookup rather than false null.
4. Make all historical gateway reads strict, including numeric selectors:
   nonce at requested state, response height/hash checks, context-preserving
   proofs, complete logs and fee history, and propagated availability errors.
   Do not use lagging index progress as the network head.
5. Keep immutable positive gateway caches useful. Bypass Stitch response/hash
   caches in the new validated adapters until identity-scoped semantic keys
   and safe admission are established. Never cache an HTTP-200 RPC error.
6. Keep unsupported streaming, writes, arbitrary search federation, and
   deployment changes out of scope. Historical ranges used by the gateway
   are assembled from individually validated heights, not generic pagination.

## Independent validation

Implementation and testing have separate owners. Add regression cases for
arbitrary/generated shard layouts, identical E across methods, later-created
accounts, replica failure, missing state/parent/proofs, wrong heights/chains,
old object hashes, incomplete searches, cross-shard ranges, and cache phases.
Run repository suites, focused race tests, vet/build, and a real Stitch process
against local fixtures. Record synthetic-fixture limitations explicitly.

Provide a read-only differential harness comparing two separately running
gateway processes: one backed by a complete archive, one by Stitch over the
same history. Compare literal requests and complete JSON-RPC outcomes while
preserving null, empty, zero, number precision, array order, and error data.
Never use the existing live test that seeds transactions on production.

Real archive/snapshot validation is a release gate. It must exercise real
execution, proofs, failed transactions, and retained-store boundaries; mocks
and restriction proxies alone do not prove native archival parity. Record any
missing prerequisite and leave this gate open rather than claiming completion.
No production enablement, deployment, or PR merge is part of this change.
