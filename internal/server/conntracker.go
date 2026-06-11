package server

import (
	"context"
	"io"
	"sync"
)

// ConnTracker tracks hijacked websocket client connections so a server's
// Shutdown can close them; http.Server.Shutdown neither waits for nor
// closes hijacked conns.
type ConnTracker struct {
	mu       sync.Mutex
	draining bool
	conns    map[io.Closer]struct{}
	handlers sync.WaitGroup
}

// NewConnTracker returns an initialised ConnTracker ready for use.
func NewConnTracker() *ConnTracker {
	return &ConnTracker{conns: make(map[io.Closer]struct{})}
}

// Track registers a live connection. Returns false once draining has begun —
// the caller must close c instead of serving it, because the sweep may
// already have run.
func (t *ConnTracker) Track(c io.Closer) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.draining {
		return false
	}
	t.conns[c] = struct{}{}
	t.handlers.Add(1)
	return true
}

// Untrack removes c from the live set and signals that its handler has
// finished. Must be called exactly once per successful Track.
func (t *ConnTracker) Untrack(c io.Closer) {
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
	t.handlers.Done()
}

// SweepAndWait sets the draining flag, force-closes every tracked
// connection, and then waits for all in-flight handlers to return.
// It returns promptly once all handlers finish, or ctx.Err() if the
// context expires first. The sweep itself runs even if ctx is already
// done. Calling SweepAndWait more than once is safe: subsequent calls
// add no work but still race handlers.Wait() against ctx.
func (t *ConnTracker) SweepAndWait(ctx context.Context) error {
	t.mu.Lock()
	t.draining = true
	for c := range t.conns {
		_ = c.Close()
	}
	t.mu.Unlock()

	done := make(chan struct{})
	go func() { t.handlers.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
