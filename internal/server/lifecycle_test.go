package server

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// fakeServer mirrors the http.Server semantics: Start blocks until the
// listener is closed (modeled here as a channel), Shutdown closes the
// listener first (unblocking Start) and then may take additional time to
// drain in-flight work.
type fakeServer struct {
	name          string
	startErr      error
	shutdownDelay time.Duration // time spent after the listener is closed
	shutdownErr   error
	shutdownIgnoresCtx bool      // if true, the drain phase sleeps even past ctx.Done

	listenerClosed chan struct{} // closed by Shutdown; unblocks Start
	shutdownCalls  atomic.Int64
}

func newFakeServer(name string) *fakeServer {
	return &fakeServer{
		name:           name,
		listenerClosed: make(chan struct{}),
	}
}

func (s *fakeServer) Name() string { return s.name }

func (s *fakeServer) Start(_ context.Context) error {
	if s.startErr != nil {
		return s.startErr
	}
	<-s.listenerClosed
	return nil
}

func (s *fakeServer) Shutdown(ctx context.Context) error {
	s.shutdownCalls.Add(1)
	// "Close the listener" — Start returns now.
	close(s.listenerClosed)
	if s.shutdownDelay > 0 {
		if s.shutdownIgnoresCtx {
			time.Sleep(s.shutdownDelay)
		} else {
			select {
			case <-time.After(s.shutdownDelay):
			case <-ctx.Done():
			}
		}
	}
	return s.shutdownErr
}

// TestDrainReturnsEarlyWhenAllShutdownsComplete verifies the bug-fix: drain
// should NOT wait the full shutdownGrace when all servers finish faster.
func TestDrainReturnsEarlyWhenAllShutdownsComplete(t *testing.T) {
	a := newFakeServer("a")
	a.shutdownDelay = 50 * time.Millisecond

	b := newFakeServer("b")
	b.shutdownDelay = 50 * time.Millisecond

	m := New(5 * time.Second) // grace far longer than the actual shutdown
	m.Add(a)
	m.Add(b)

	done := make(chan struct{})
	go func() {
		_ = m.Run(context.Background())
		close(done)
	}()

	// Give servers a moment to start.
	time.Sleep(50 * time.Millisecond)

	// Send a SIGTERM-equivalent (using SIGINT to avoid noisily killing the
	// test process; both go through the same path).
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
		t.Fatalf("kill: %v", err)
	}

	start := time.Now()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Manager.Run did not return within 2s after SIGINT")
	}
	elapsed := time.Since(start)
	// Sanity bound: should take well under the 5s grace. Allow generous slack
	// for CI scheduling jitter.
	if elapsed > 1*time.Second {
		t.Errorf("drain took %v; expected < 1s (grace is 5s but shutdowns finish in 50ms)", elapsed)
	}
	if a.shutdownCalls.Load() != 1 || b.shutdownCalls.Load() != 1 {
		t.Errorf("expected both servers shutdown once; got a=%d b=%d",
			a.shutdownCalls.Load(), b.shutdownCalls.Load())
	}
}

// TestDrainOnPeerServerError reproduces the hang scenario: when one server
// returns an error from Start (e.g. an admin port conflict), errgroup cancels
// gctx and the signal handler would previously exit without draining peers.
// The fix: signal handler drains on gctx.Done() too.
func TestDrainOnPeerServerError(t *testing.T) {
	bad := newFakeServer("bad")
	bad.startErr = errors.New("bind: address already in use")

	good := newFakeServer("good")
	good.shutdownDelay = 20 * time.Millisecond

	m := New(2 * time.Second)
	m.Add(bad)
	m.Add(good)

	done := make(chan error, 1)
	go func() { done <- m.Run(context.Background()) }()

	select {
	case err := <-done:
		if err == nil || !errors.Is(err, bad.startErr) {
			t.Errorf("expected the bad server's startErr to surface; got %v", err)
		}
		if good.shutdownCalls.Load() != 1 {
			t.Errorf("expected the good server to be drained after the bad one failed; shutdown calls=%d", good.shutdownCalls.Load())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Manager.Run hung after a peer server failed — the bug we're fixing")
	}
}

// TestDrainTimeoutLogsRemaining ensures drain still returns when a server's
// Shutdown ignores the context — and the operator can identify which server.
// (We can't easily assert on log content here without plumbing, so we just
// check drain still returns within ~grace and the slow server is still marked
// having had Shutdown called.)
func TestDrainTimeoutHonored(t *testing.T) {
	slow := newFakeServer("slow")
	slow.shutdownDelay = 5 * time.Second
	slow.shutdownIgnoresCtx = true

	m := New(100 * time.Millisecond)
	m.Add(slow)

	done := make(chan struct{})
	go func() {
		_ = m.Run(context.Background())
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
		t.Fatalf("kill: %v", err)
	}

	// Run should return shortly after the 100ms grace expires even though
	// the slow server's Shutdown sleeps for 5s.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Manager.Run did not return after grace expired")
	}
	if slow.shutdownCalls.Load() != 1 {
		t.Errorf("expected Shutdown called on slow server; got %d", slow.shutdownCalls.Load())
	}
}
