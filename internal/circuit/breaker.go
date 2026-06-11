// Package circuit implements per-(backend,protocol) circuit breakers.
//
// State transitions:
//
//	closed → open       when error_rate ≥ threshold over ≥ min_requests in window
//	open → half-open    on Acquire after open_duration (caller becomes the canary)
//	half-open → closed  on a successful canary
//	half-open → open    on a failed canary (with exponential backoff)
//
// The API is split between filtering and admission. Allow is read-only —
// the selector calls it to filter candidates and it never transitions
// state or consumes anything. Acquire admits one request: it performs the
// open→half-open transition once the cooldown has elapsed and claims the
// single canary slot, so at most one canary is in flight per half-open
// period. Record resolves outcomes and releases the slot. Release abandons
// an admission without an outcome — no sample, no transition — freeing a
// claimed canary slot for requests that ended without indicting anyone
// (e.g. the client vanished mid-flight).
package circuit

import (
	"sync"
	"sync/atomic"
	"time"
)

// State enumerates breaker states. Values match the Prometheus gauge.
type State int32

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half_open"
	case StateOpen:
		return "open"
	default:
		return "unknown"
	}
}

// Policy configures a breaker.
type Policy struct {
	ErrorThreshold float64       // [0, 1]
	MinRequests    int           // minimum samples before tripping
	OpenDuration   time.Duration // cooldown before half-open
	WindowSize     int           // ring buffer size for samples; default 64
}

func (p Policy) withDefaults() Policy {
	if p.WindowSize <= 0 {
		p.WindowSize = 64
	}
	return p
}

// Breaker is a single circuit. Safe for concurrent use.
//
// Allow is lock-free, and so is Acquire's closed-state fast path (a single
// atomic load). All state transitions — open→half-open in Acquire,
// trip/close in Record — are serialized under mu so concurrent records
// cannot double-apply backoff or clobber openedAt.
type Breaker struct {
	policy        Policy
	state         atomic.Int32
	openedAt      atomic.Int64 // unix nano of the last trip
	openedBackoff atomic.Int64 // current cooldown nanos (grows on repeated trips)
	canary        atomic.Bool  // half-open canary slot; claimed by Acquire, released by Record

	mu      sync.Mutex // guards samples and all state transitions
	samples []bool     // true=success, false=failure
	idx     int
	count   int
}

func NewBreaker(p Policy) *Breaker {
	p = p.withDefaults()
	return &Breaker{
		policy:  p,
		samples: make([]bool, p.WindowSize),
	}
}

// Allow reports whether a request could currently be sent. It is read-only
// — safe for the selector's candidate filtering — and never transitions
// state or consumes the canary slot:
//
//	closed                      → true
//	open, cooldown elapsed      → true (an Acquire would admit a canary)
//	open, cooldown running      → false
//	half-open, slot free        → true
//	half-open, canary in flight → false
func (b *Breaker) Allow() bool {
	switch State(b.state.Load()) {
	case StateClosed:
		return true
	case StateHalfOpen:
		return !b.canary.Load()
	case StateOpen:
		return b.cooldownElapsed()
	}
	return true
}

// Acquire admits one request; callers must report the outcome via Record so
// a claimed canary slot is released. Closed circuits admit without claiming
// anything. On an open circuit whose cooldown has elapsed, the caller
// becomes the single half-open canary; otherwise Acquire returns false.
func (b *Breaker) Acquire() bool {
	if State(b.state.Load()) == StateClosed {
		return true
	}
	// Cold path: open or probing. Serialize with Record's transitions so
	// exactly one canary is admitted per half-open period.
	b.mu.Lock()
	defer b.mu.Unlock()
	switch State(b.state.Load()) {
	case StateHalfOpen:
		return b.canary.CompareAndSwap(false, true)
	case StateOpen:
		if !b.cooldownElapsed() {
			return false
		}
		if !b.canary.CompareAndSwap(false, true) {
			return false
		}
		b.state.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen))
		return true
	}
	return true // closed concurrently with the fast-path load
}

// Release abandons an admission obtained via Acquire without recording an
// outcome: no sample is added and no state transition happens; a claimed
// half-open canary slot is freed so another caller may probe. No-op when
// the breaker is not half-open or the slot is unclaimed. For requests
// whose outcome says nothing about the backend — e.g. the client vanished
// mid-flight, or a hedge/broadcast loser cancelled after a winner.
//
// Release may free a slot claimed by a different in-flight canary when
// admissions interleave across a trip/re-probe cycle — the same
// stale-evidence tolerance Record already has.
func (b *Breaker) Release() {
	if State(b.state.Load()) == StateClosed {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if State(b.state.Load()) == StateHalfOpen {
		b.canary.Store(false)
	}
}

// Record reports the outcome of a request. Closed-state failures may trip
// the breaker; resolving a half-open canary — success or failure — closes
// or re-trips it and releases the canary slot. Open-state records are
// samples only: with every sender holding an admission from Acquire, an
// outcome arriving while open is by definition stale pre-trip evidence.
func (b *Breaker) Record(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.samples[b.idx] = success
	b.idx = (b.idx + 1) % len(b.samples)
	if b.count < len(b.samples) {
		b.count++
	}

	switch State(b.state.Load()) {
	case StateClosed:
		if b.count < b.policy.MinRequests {
			return
		}
		failures := 0
		for i := 0; i < b.count; i++ {
			if !b.samples[i] {
				failures++
			}
		}
		if float64(failures)/float64(b.count) >= b.policy.ErrorThreshold {
			b.tripLocked()
		}
	case StateHalfOpen:
		if success {
			b.closeLocked()
		} else {
			b.tripLocked()
		}
	case StateOpen:
		// A late result from a request dispatched pre-trip — even one
		// resolving only after the cooldown elapsed: sample it, change
		// nothing. Open-state resolution belongs to canaries admitted via
		// Acquire (handled under StateHalfOpen above); an un-admitted
		// outcome never closes or re-trips the breaker.
	}
}

func (b *Breaker) State() State { return State(b.state.Load()) }

// cooldownNanos returns the effective open-state cooldown.
func (b *Breaker) cooldownNanos() int64 {
	if c := b.openedBackoff.Load(); c > 0 {
		return c
	}
	return b.policy.OpenDuration.Nanoseconds()
}

func (b *Breaker) cooldownElapsed() bool {
	return time.Now().UnixNano()-b.openedAt.Load() >= b.cooldownNanos()
}

// tripLocked moves the breaker to open. A fresh trip from closed starts at
// the base OpenDuration; a re-trip (a failed half-open canary) doubles the
// cooldown up to 8× base. Callers hold mu. openedAt is stored before the
// state so a concurrent Allow never sees the open state with a stale
// timestamp.
func (b *Breaker) tripLocked() {
	base := b.policy.OpenDuration.Nanoseconds()
	if State(b.state.Load()) == StateClosed {
		b.openedBackoff.Store(base)
	} else {
		next := b.cooldownNanos() * 2
		if next > base*8 {
			next = base * 8
		}
		b.openedBackoff.Store(next)
	}
	b.openedAt.Store(time.Now().UnixNano())
	b.state.Store(int32(StateOpen))
	b.canary.Store(false)
}

// closeLocked returns the breaker to closed, releasing the canary slot and
// resetting the backoff and sample window. Callers hold mu.
func (b *Breaker) closeLocked() {
	b.state.Store(int32(StateClosed))
	b.openedBackoff.Store(0)
	b.canary.Store(false)
	for i := range b.samples {
		b.samples[i] = false
	}
	b.idx = 0
	b.count = 0
}
