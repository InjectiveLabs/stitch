// Package circuit implements per-(backend,protocol) circuit breakers.
//
// State transitions:
//
//	closed → open       when error_rate ≥ threshold over ≥ min_requests in window
//	open → half-open    after open_duration
//	half-open → closed  on a successful canary
//	half-open → open    on a failed canary (with exponential backoff)
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
type Breaker struct {
	policy    Policy
	state     atomic.Int32
	openedAt  atomic.Int64 // unix nano
	openedBackoff atomic.Int64 // current cooldown nanos (grows on repeated trips)

	mu      sync.Mutex
	samples []bool // true=success, false=failure
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

// Allow reports whether the next request should be sent. It also performs
// the open→half-open transition when the cooldown has elapsed.
func (b *Breaker) Allow() bool {
	switch State(b.state.Load()) {
	case StateClosed:
		return true
	case StateHalfOpen:
		return true
	case StateOpen:
		cooldown := b.openedBackoff.Load()
		if cooldown == 0 {
			cooldown = b.policy.OpenDuration.Nanoseconds()
		}
		if time.Now().UnixNano()-b.openedAt.Load() >= cooldown {
			// transition to half-open if not yet
			b.state.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen))
			return true
		}
		return false
	}
	return true
}

// Record reports the outcome of a request. Failures may trip the breaker;
// successes may close it.
func (b *Breaker) Record(success bool) {
	b.mu.Lock()
	b.samples[b.idx] = success
	b.idx = (b.idx + 1) % len(b.samples)
	if b.count < len(b.samples) {
		b.count++
	}
	failures := 0
	for i := 0; i < b.count; i++ {
		if !b.samples[i] {
			failures++
		}
	}
	count := b.count
	b.mu.Unlock()

	st := State(b.state.Load())
	switch st {
	case StateClosed:
		if count >= b.policy.MinRequests {
			rate := float64(failures) / float64(count)
			if rate >= b.policy.ErrorThreshold {
				b.trip()
			}
		}
	case StateHalfOpen:
		if success {
			b.close()
		} else {
			b.trip()
		}
	}
}

func (b *Breaker) State() State { return State(b.state.Load()) }

func (b *Breaker) trip() {
	if b.state.Swap(int32(StateOpen)) == int32(StateOpen) {
		// Already open; double the backoff up to 8× base.
		base := b.policy.OpenDuration.Nanoseconds()
		cur := b.openedBackoff.Load()
		if cur == 0 {
			cur = base
		}
		next := cur * 2
		if next > base*8 {
			next = base * 8
		}
		b.openedBackoff.Store(next)
	} else {
		b.openedBackoff.Store(b.policy.OpenDuration.Nanoseconds())
	}
	b.openedAt.Store(time.Now().UnixNano())
}

func (b *Breaker) close() {
	b.state.Store(int32(StateClosed))
	b.openedBackoff.Store(0)
	b.mu.Lock()
	for i := range b.samples {
		b.samples[i] = false
	}
	b.idx = 0
	b.count = 0
	b.mu.Unlock()
}
