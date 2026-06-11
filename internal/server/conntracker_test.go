package server

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// nopCloser is a no-op io.Closer for use in tests.
type nopCloser struct{ closed chan struct{} }

func newNopCloser() *nopCloser { return &nopCloser{closed: make(chan struct{})} }
func (c *nopCloser) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

func TestConnTrackerTrackAfterDrainReturnsFalse(t *testing.T) {
	tr := NewConnTracker()

	// Trigger draining by sweeping with an already-cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = tr.SweepAndWait(ctx)

	c := newNopCloser()
	if tr.Track(c) {
		t.Fatal("Track returned true after drain; expected false")
	}
}

func TestConnTrackerSweepAndWaitReturnsWhenHandlersFinish(t *testing.T) {
	tr := NewConnTracker()

	c := newNopCloser()
	if !tr.Track(c) {
		t.Fatal("Track unexpectedly returned false before drain")
	}

	// Untrack in the background after a brief delay, simulating a
	// handler that finishes quickly once its conn is closed.
	go func() {
		<-c.closed // wait until SweepAndWait closes the conn
		tr.Untrack(c)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tr.SweepAndWait(ctx); err != nil {
		t.Fatalf("SweepAndWait returned error: %v", err)
	}
}

func TestConnTrackerSweepAndWaitReturnsCtxErrorWhenHandlerNeverFinishes(t *testing.T) {
	tr := NewConnTracker()

	c := newNopCloser()
	if !tr.Track(c) {
		t.Fatal("Track unexpectedly returned false before drain")
	}
	// Intentionally never call Untrack — simulates a stuck handler.

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := tr.SweepAndWait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded; got %v", err)
	}

	// Clean up so the tracker's WaitGroup doesn't hold the test goroutine.
	tr.Untrack(c)
}

func TestConnTrackerIdempotentDoubleSweep(t *testing.T) {
	tr := NewConnTracker()

	c := newNopCloser()
	if !tr.Track(c) {
		t.Fatal("Track unexpectedly returned false before drain")
	}
	go func() {
		<-c.closed
		tr.Untrack(c)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := tr.SweepAndWait(ctx); err != nil {
		t.Fatalf("first SweepAndWait: %v", err)
	}
	// Second sweep: draining already set, conns map is empty, handlers.Wait
	// returns immediately since all handlers are done.
	if err := tr.SweepAndWait(ctx); err != nil {
		t.Fatalf("second SweepAndWait: %v", err)
	}

	// Track must still return false after double sweep.
	c2 := newNopCloser()
	if tr.Track(c2) {
		t.Fatal("Track returned true after double sweep; expected false")
	}
}

// Ensure *nopCloser satisfies io.Closer (compile-time check).
var _ io.Closer = (*nopCloser)(nil)
