// Package subscription owns the resume-aware logic for streaming
// subscriptions across stitch's listeners.
//
// The fundamental abstraction is a Cursor: a monotonic position in a
// stream of events. Every event the hub forwards advances the cursor.
// On upstream failure, the hub re-opens to a new backend and discards
// every event whose cursor is ≤ the last-delivered position. The client
// connection sees the rejoin as a continuous stream.
package subscription

import (
	"strings"
)

// Kind enumerates the subscription kinds stitch knows how to resume.
// Anything else falls back to non-resumable relay.
type Kind int

const (
	KindUnknown Kind = iota
	KindEthNewHeads
	KindEthLogs
	KindEthPendingTransactions
	KindEthSyncing
)

func (k Kind) String() string {
	switch k {
	case KindEthNewHeads:
		return "newHeads"
	case KindEthLogs:
		return "logs"
	case KindEthPendingTransactions:
		return "newPendingTransactions"
	case KindEthSyncing:
		return "syncing"
	}
	return "unknown"
}

// Resumable reports whether this kind can survive a backend swap.
// `newPendingTransactions` cannot — the mempool is per-node.
// `syncing` is informational, not state — we treat it as non-resumable.
func (k Kind) Resumable() bool {
	switch k {
	case KindEthNewHeads, KindEthLogs:
		return true
	}
	return false
}

// ParseEthKind interprets the first parameter of eth_subscribe.
func ParseEthKind(s string) Kind {
	switch strings.Trim(s, `"`) {
	case "newHeads":
		return KindEthNewHeads
	case "logs":
		return KindEthLogs
	case "newPendingTransactions":
		return KindEthPendingTransactions
	case "syncing":
		return KindEthSyncing
	}
	return KindUnknown
}

// Cursor is a monotonic position in a subscription's event stream.
// Two cursors are comparable; a strictly-greater cursor means "later in
// the stream". The Zero cursor sorts before any real event.
//
// Per Injective EVM JSON-RPC the relevant ordering keys are:
//
//	newHeads:  block number
//	logs:      (block number, tx index within block, log index within tx)
//
// We carry all three components so the same Cursor type works for both.
// For newHeads, TxIndex/LogIndex stay zero.
type Cursor struct {
	Height   int64
	TxIndex  int64
	LogIndex int64
}

// Less reports whether c is strictly earlier than other.
func (c Cursor) Less(other Cursor) bool {
	if c.Height != other.Height {
		return c.Height < other.Height
	}
	if c.TxIndex != other.TxIndex {
		return c.TxIndex < other.TxIndex
	}
	return c.LogIndex < other.LogIndex
}

// LessEq reports whether c is earlier than or equal to other.
func (c Cursor) LessEq(other Cursor) bool {
	return c == other || c.Less(other)
}

// IsZero reports whether the cursor has not yet advanced.
func (c Cursor) IsZero() bool {
	return c == Cursor{}
}

// Advance returns true if next strictly advances the cursor; the receiver
// is updated when so. Used in the per-client dedup loop on resume.
func (c *Cursor) Advance(next Cursor) bool {
	if c.LessEq(next) && *c != next {
		*c = next
		return true
	}
	if c.IsZero() {
		*c = next
		return true
	}
	return false
}
