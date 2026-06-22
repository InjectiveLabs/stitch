package cmt_rpc

import "github.com/decentrio/stitch/internal/types"

// MethodSpec describes how one CometBFT RPC method is routed. Phase 1 keeps
// the manifest as in-code data; later phases may load it from YAML.
type MethodSpec struct {
	Name           string
	Class          types.MethodClass
	HeightParam    string // param name for height (uri+json-rpc)
	HashParam      string // param name for hash
	HeightOptional bool   // if true, treat absent height as latest
	Idempotent     bool
	Cacheable      bool
}

// Manifest is the canonical method table for CometBFT RPC. Reference for
// Injective: query-reference §6 (CometBFT RPC).
var Manifest = func() map[string]MethodSpec {
	m := make(map[string]MethodSpec)
	add := func(s MethodSpec) {
		if s.HeightOptional {
			s.Class = types.ClassByHeight
		}
		m[s.Name] = s
	}

	// Status / discovery — always latest.
	for _, name := range []string{"health", "status", "net_info", "genesis", "genesis_chunked", "dump_consensus_state", "consensus_state", "abci_info"} {
		add(MethodSpec{Name: name, Class: types.ClassLatest, Idempotent: true})
	}

	// Height-keyed reads.
	for _, name := range []string{"block", "block_results", "header", "commit", "validators", "consensus_params"} {
		add(MethodSpec{
			Name:           name,
			HeightParam:    "height",
			HeightOptional: true, // absent height = latest
			Idempotent:     true,
			Cacheable:      true,
		})
	}

	// abci_query: height in params, optional.
	add(MethodSpec{
		Name:           "abci_query",
		HeightParam:    "height",
		HeightOptional: true,
		Idempotent:     true,
		Cacheable:      true,
	})

	// Hash-keyed reads (phase 6 adds memoization; phase 1 fans out).
	add(MethodSpec{Name: "block_by_hash", HashParam: "hash", Class: types.ClassByHash, Idempotent: true, Cacheable: true})
	add(MethodSpec{Name: "header_by_hash", HashParam: "hash", Class: types.ClassByHash, Idempotent: true, Cacheable: true})
	add(MethodSpec{Name: "tx", HashParam: "hash", Class: types.ClassByHash, Idempotent: true, Cacheable: true})

	// Search methods route by tx.height/block.height when their event query
	// contains block-height constraints; otherwise they fall back to latest.
	add(MethodSpec{Name: "tx_search", Class: types.ClassLatest, Idempotent: true})
	add(MethodSpec{Name: "block_search", Class: types.ClassLatest, Idempotent: true})

	// Latest-bound, mempool-affected, non-cacheable.
	for _, name := range []string{"check_tx", "unconfirmed_txs", "num_unconfirmed_txs"} {
		add(MethodSpec{Name: name, Class: types.ClassLatest, Idempotent: true})
	}

	// Range query — routes by minHeight/maxHeight when supplied.
	add(MethodSpec{Name: "blockchain", Class: types.ClassLatest, Idempotent: true})

	// Tx broadcast: phase 1 routes to a single healthy backend; phase 6
	// converts these to fan-out.
	for _, name := range []string{"broadcast_tx_sync", "broadcast_tx_async", "broadcast_tx_commit"} {
		add(MethodSpec{Name: name, Class: types.ClassBroadcast, Idempotent: false})
	}

	return m
}()

// Lookup returns the spec for method name, or a default Latest spec if
// unknown. Unknown methods are forwarded but not retried, since we cannot
// be sure they are idempotent.
func Lookup(name string) MethodSpec {
	if s, ok := Manifest[name]; ok {
		return s
	}
	return MethodSpec{Name: name, Class: types.ClassLatest, Idempotent: false}
}
