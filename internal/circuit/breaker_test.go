package circuit

import (
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
		t.Fatal("Allow() should return true after cooldown (half-open)")
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
	if !b.Allow() {
		t.Fatal("first cooldown should expire")
	}
	b.Record(false) // canary fails
	if b.State() != StateOpen {
		t.Fatalf("expected re-open, got %s", b.State())
	}
}
