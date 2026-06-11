package circuit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBreakerTripsOnHighErrorRate(t *testing.T) {
	b := NewBreaker(Policy{
		ErrorThreshold: 0.5,
		MinRequests:    4,
		OpenDuration:   50 * time.Millisecond,
		WindowSize:     8,
	})
	if !b.Allow() {
		t.Fatal("breaker should start closed")
	}
	for i := 0; i < 4; i++ {
		b.Record(false)
	}
	if b.State() != StateOpen {
		t.Fatalf("breaker should be open after 4 failures, got %s", b.State())
	}
	if b.Allow() {
		t.Fatal("Allow() should return false while open and cooldown unexpired")
	}
}

func TestBreakerHalfOpenThenClose(t *testing.T) {
	b := NewBreaker(Policy{
		ErrorThreshold: 0.5,
		MinRequests:    4,
		OpenDuration:   10 * time.Millisecond,
		WindowSize:     8,
	})
	for i := 0; i < 4; i++ {
		b.Record(false)
	}
	if b.State() != StateOpen {
		t.Fatalf("expected open, got %s", b.State())
	}
	time.Sleep(15 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("Allow() should return true after cooldown")
	}
	if !b.Acquire() {
		t.Fatal("Acquire() should claim the canary after cooldown")
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("expected half_open, got %s", b.State())
	}
	b.Record(true)
	if b.State() != StateClosed {
		t.Fatalf("expected closed after canary success, got %s", b.State())
	}
}

func TestBreakerHalfOpenFailureBacksOff(t *testing.T) {
	b := NewBreaker(Policy{
		ErrorThreshold: 0.5,
		MinRequests:    4,
		OpenDuration:   5 * time.Millisecond,
		WindowSize:     8,
	})
	for i := 0; i < 4; i++ {
		b.Record(false)
	}
	time.Sleep(8 * time.Millisecond)
	if !b.Acquire() {
		t.Fatal("first cooldown should expire")
	}
	b.Record(false) // canary fails
	if b.State() != StateOpen {
		t.Fatalf("expected re-open, got %s", b.State())
	}
	if got, want := b.openedBackoff.Load(), (10 * time.Millisecond).Nanoseconds(); got != want {
		t.Errorf("failed canary should double the cooldown: got %dns want %dns", got, want)
	}
}

// Allow must stay side-effect-free: the selector calls it to filter
// candidates and must never consume the canary slot or transition state.
func TestBreakerAllowIsReadOnly(t *testing.T) {
	b := NewBreaker(Policy{
		ErrorThreshold: 0.5,
		MinRequests:    4,
		OpenDuration:   10 * time.Millisecond,
		WindowSize:     8,
	})
	for i := 0; i < 4; i++ {
		b.Record(false)
	}
	time.Sleep(15 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatal("Allow() should return true once the cooldown elapsed")
		}
	}
	if b.State() != StateOpen {
		t.Fatalf("Allow() must not transition state; got %s", b.State())
	}
	if b.canary.Load() {
		t.Fatal("Allow() must not claim the canary slot")
	}
}

// Exactly one of N concurrent Acquire calls may win the half-open canary.
func TestBreakerSingleCanary(t *testing.T) {
	b := NewBreaker(Policy{
		ErrorThreshold: 0.5,
		MinRequests:    4,
		OpenDuration:   10 * time.Millisecond,
		WindowSize:     8,
	})
	for i := 0; i < 4; i++ {
		b.Record(false)
	}
	time.Sleep(15 * time.Millisecond)

	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Acquire() {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 1 {
		t.Fatalf("expected exactly 1 canary admitted, got %d", admitted.Load())
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("expected half_open, got %s", b.State())
	}
	if b.Allow() {
		t.Fatal("Allow() should report false while the canary slot is taken")
	}
}

// A resolved canary — success or failure — releases the slot.
func TestBreakerCanaryReleasedOnResolve(t *testing.T) {
	pol := Policy{
		ErrorThreshold: 0.5,
		MinRequests:    4,
		OpenDuration:   10 * time.Millisecond,
		WindowSize:     8,
	}

	// Success: half-open → closed, slot free, backoff reset.
	b := NewBreaker(pol)
	for i := 0; i < 4; i++ {
		b.Record(false)
	}
	time.Sleep(15 * time.Millisecond)
	if !b.Acquire() {
		t.Fatal("canary should be admitted")
	}
	b.Record(true)
	if b.State() != StateClosed {
		t.Fatalf("expected closed, got %s", b.State())
	}
	if b.canary.Load() {
		t.Fatal("slot should be released on success")
	}
	if b.openedBackoff.Load() != 0 {
		t.Fatal("backoff should reset on close")
	}
	if !b.Acquire() {
		t.Fatal("closed breaker should admit freely")
	}

	// Failure: half-open → open, slot free, next canary admitted after the
	// (doubled) cooldown.
	b = NewBreaker(pol)
	for i := 0; i < 4; i++ {
		b.Record(false)
	}
	time.Sleep(15 * time.Millisecond)
	if !b.Acquire() {
		t.Fatal("canary should be admitted")
	}
	b.Record(false)
	if b.State() != StateOpen {
		t.Fatalf("expected open, got %s", b.State())
	}
	if b.canary.Load() {
		t.Fatal("slot should be released on failure")
	}
	if b.Acquire() {
		t.Fatal("re-opened breaker should reject before the new cooldown elapses")
	}
	time.Sleep(25 * time.Millisecond) // doubled cooldown = 20ms
	if !b.Acquire() {
		t.Fatal("next canary should be admitted after the doubled cooldown")
	}
}

// Concurrent failure records must produce exactly one trip: backoff stays at
// the base OpenDuration and openedAt is not clobbered by a second trip.
func TestBreakerConcurrentFailuresTripOnce(t *testing.T) {
	b := NewBreaker(Policy{
		ErrorThreshold: 0.5,
		MinRequests:    4,
		OpenDuration:   time.Minute,
		WindowSize:     64,
	})
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Record(false)
		}()
	}
	wg.Wait()
	if b.State() != StateOpen {
		t.Fatalf("expected open, got %s", b.State())
	}
	if got, want := b.openedBackoff.Load(), time.Minute.Nanoseconds(); got != want {
		t.Errorf("concurrent records doubled the backoff: got %dns want %dns", got, want)
	}
}

// Outcomes of requests dispatched before a trip arrive while the breaker is
// open and the cooldown is still running: they are samples only.
func TestBreakerLateRecordWhileOpenIsIgnored(t *testing.T) {
	b := NewBreaker(Policy{
		ErrorThreshold: 0.5,
		MinRequests:    4,
		OpenDuration:   time.Minute,
		WindowSize:     8,
	})
	for i := 0; i < 4; i++ {
		b.Record(false)
	}
	b.Record(true)
	if b.State() != StateOpen {
		t.Fatalf("late success must not close an open breaker; got %s", b.State())
	}
}

// Callers that gate on Allow alone (no Acquire) send once the cooldown has
// elapsed; their outcome must still resolve the breaker.
func TestBreakerOpenRecordAfterCooldownResolves(t *testing.T) {
	pol := Policy{
		ErrorThreshold: 0.5,
		MinRequests:    4,
		OpenDuration:   10 * time.Millisecond,
		WindowSize:     8,
	}

	b := NewBreaker(pol)
	for i := 0; i < 4; i++ {
		b.Record(false)
	}
	time.Sleep(15 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("cooldown should have elapsed")
	}
	b.Record(true) // Allow-gated caller's success, no Acquire
	if b.State() != StateClosed {
		t.Fatalf("post-cooldown success should close, got %s", b.State())
	}

	b = NewBreaker(pol)
	for i := 0; i < 4; i++ {
		b.Record(false)
	}
	time.Sleep(15 * time.Millisecond)
	b.Record(false) // Allow-gated caller's failure, no Acquire
	if b.State() != StateOpen {
		t.Fatalf("post-cooldown failure should re-trip, got %s", b.State())
	}
	if got, want := b.openedBackoff.Load(), (20 * time.Millisecond).Nanoseconds(); got != want {
		t.Errorf("post-cooldown failure should double the cooldown: got %dns want %dns", got, want)
	}
}
