package cache

import "sync/atomic"

// HeadProvider hands out the latest known chain head. Stitch's wiring
// glues this to health.Registry.MaxHead() at startup, so the cache layer
// has no compile-time dependency on health.
type HeadProvider func() int64

// AtomicHead is a tiny goroutine-safe head value, useful for tests that
// don't want to construct a full health registry.
type AtomicHead struct{ v atomic.Int64 }

func (a *AtomicHead) Set(h int64) { a.v.Store(h) }

func (a *AtomicHead) Get() int64 { return a.v.Load() }

// IsCacheableHeight reports whether a height-keyed response can be
// cached given the current head and configured confirmation depth.
//
// We only cache heights that are far enough behind head to be safe from
// reorgs. Tendermint chains finalize on commit so the depth can be 0,
// but EVM-style chains can in theory reorg shallow blocks; defaulting
// to 100 (the value in the design) gives operators a knob.
func IsCacheableHeight(height, head, confirmationDepth int64) bool {
	if height <= 0 {
		return false
	}
	if head <= 0 {
		return false
	}
	return height <= head-confirmationDepth
}

// IsImmutableMethod returns true for methods whose result, once
// returned, can be cached forever (no reorg, no mutation). Examples are
// hash-keyed reads of finalized data: tx-by-hash, block-by-hash. Height
// gating doesn't apply because the hash itself uniquely identifies the
// answer.
func IsImmutableMethod(method string) bool {
	switch method {
	case "tx", "block_by_hash", "header_by_hash",
		"eth_getTransactionByHash", "eth_getTransactionReceipt",
		"eth_getBlockByHash", "eth_getBlockTransactionCountByHash",
		"eth_getTransactionByBlockHashAndIndex":
		return true
	}
	return false
}
