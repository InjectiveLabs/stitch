# EVM archive routing

Tracked by ID-1573 under ID-1568. See [the plan](archive-parity-plan.md).

## One logical archive

Configure `archive.evm_start_height` and `archive.cosmos_chain_id` with a
verified EVM history boundary and Cosmos chain identity. Configure the same
`WEB3INJ_ARCHIVE_START_HEIGHT` in the gateway and enable
`WEB3INJ_STITCH_BACKEND` only after verification. This boundary is independent
of indexing backfill, shard layout, health, drain state, and method capability.
There is no default activation height. An archive profile requires restart to
change; backend topology can still reload.

Every `earliest` request means the same configured height E. Missing proofs,
state, results, or a trace's parent snapshot must not advance it. A zero or
empty result and an absent account are valid at E; they do not initiate a
search for the account's first appearance. Reads at E and derived heights
such as E−1 may use different backends. The backend actually executing a trace
still needs that operation's real execution dependencies.

## Gateway handshake

Call `/injective.evm.v1.Query/Params` with `stitch: archive-profile` to read
the configured contract without depending on upstream liveness. The response
has an empty Params payload and these metadata keys:

- `x-stitch-earliest-height`: configured E
- `x-cosmos-block-height`: the same E
- `x-stitch-cosmos-chain-id`: configured Cosmos chain identity

The gateway checks this profile at startup. Runtime resolution is local,
allowing complete immutable cache entries to remain useful during an outage.
The existing `stitch: earliest` marker still makes a real state query at E.
Capability values remain accepted for rollout compatibility but never change
the selected height. Actual dependent queries validate their own requirements.
No physical-backend affinity is needed in the new contract.

## Historical reads

Supported read-only unary gRPC calls are buffered before publishing a reply.
Unavailable state, transport failure, or an invalid returned height can retry
another eligible replica at the identical height. Valid application errors
are preserved; account absence requires a same-snapshot witness. The gRPC
endpoint's own GetNodeInfo response witnesses Cosmos chain identity without
downloading a block for every state read.

With an archive profile, Comet block/header/commit/results, ABCI,
consensus-parameter, validator, and height-qualified transaction searches
validate historical responses before release. The same HTTP endpoint's status
witnesses chain identity. Block responses additionally carry the requested
height and chain; ABCI success must report the requested height. HTTP-200
missing-history errors are not successful data. Legacy response and hash
caches are bypassed for these adapters, preventing parameter collisions and
cached routing/errors from bypassing validation.

These checks establish endpoint chain identity and response consistency, not
cryptographic proof that every configured replica is on the canonical fork.
Operators must verify archive provenance. Real proof/trace and cross-protocol
identity validation remains part of the parity release gate.

## Unknown block and Ethereum transaction hashes

`block_by_hash` searches archive candidates, including old bounded shards.
A positive result must match the requested block ID, chain, and configured
coverage. A global miss requires successful lookup coverage across the full
logical interval E through the observed archive head. A drained or unhealthy
sole owner leaves the search incomplete; a healthy equivalent replica can
cover the same interval. Coverage holes never count as absence.

The narrow `tx_search` predicate
`ethereum_tx.ethereumTxHash='<hash>'` is searched across historical shards.
Each query is constrained to the shard's interval. Results validate transaction
bytes against the Comet hash and the requested Ethereum hash event, then merge
in height/index order with overlap deduplication and global pagination.
Conflicting replica results, missing required intervals, or truncated searches
fail explicitly. The gateway must additionally decode the actual EVM
transaction and verify its hash: event metadata alone is not sufficient.

An empty event search does not establish complete Ethereum transaction absence;
failed transactions may not emit the event. The gateway requires a complete
history index to answer that case definitively. Generic Comet search federation,
search proofs, header-by-hash federation, and transaction broadcast are outside
this adapter's scope.

## Bounds and validation

Comet archive operations have a 30-second overall deadline and bounded attempt
timeouts. At most four HTTP attempts execute concurrently per listener.
Block/results responses are capped at 64 MiB; transaction search at 8 MiB;
status at 64 KiB. Searches accept at most 256 candidates and 100 distinct
exact-hash matches with an 8 MiB aggregate payload budget. Requests exceeding
these budgets fail rather than return partial data. No response is released
until validated.

Run `go test ./...`, focused/full race tests, vet, and the cross-service fixture
tests. Synthetic fixtures establish regression behavior, not complete native
archive equivalence. Before enablement, run the read-only differential corpus
against a complete archive and the same history served through real retained
shards, including failed transactions, genuine proofs, traces, and cache phases.
Do not use illustrative discussion heights as deployment configuration.
