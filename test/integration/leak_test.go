package integration

import (
	"net/http"
	"runtime"
	"testing"
	"time"
)

// TestNoGoroutineLeakAcrossSetupTeardown drives a setup → handful of
// requests → teardown cycle and asserts the goroutine count returns to
// roughly the baseline. A leak in the forwarder, listener, or session
// teardown paths would manifest as a steady-state climb across cycles.
//
// The tolerance is deliberately loose — Go's runtime spawns a few
// transient goroutines (GC sweep, runtime locks, http.idleConn reaper)
// that we don't want to flake on. What we actually catch are leaks of
// O(N) per cycle, which is what regression-prone subscription / stream
// teardown bugs look like.
func TestNoGoroutineLeakAcrossSetupTeardown(t *testing.T) {
	if testing.Short() {
		t.Skip("leak: skipping under -short")
	}

	// One full cycle to warm the runtime: HTTP transport pools, etc.
	doCycle(t)
	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	baseline := runtime.NumGoroutine()
	const cycles = 5
	for i := 0; i < cycles; i++ {
		doCycle(t)
	}
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	runtime.GC() // give finalizers two passes

	final := runtime.NumGoroutine()
	delta := final - baseline
	t.Logf("goroutine baseline=%d final=%d delta=%d (after %d cycles)", baseline, final, delta, cycles)

	// Tolerance: 20 goroutines of slack across 5 cycles. Real leaks would
	// show a slope of N per cycle, blowing past this immediately.
	if delta > 20 {
		t.Errorf("goroutine count grew by %d over %d cycles (likely leak)", delta, cycles)
	}
}

// doCycle stands up the same rig as the smoke test, drives ~5 requests,
// and tears down. We exercise both the cmt_rpc and cosmos_rest paths so
// any leak in either listener's handler stack would show up.
func doCycle(t *testing.T) {
	rig := setup(t)
	defer rig.close()

	for i := 0; i < 5; i++ {
		resp, err := http.Get(rig.rpc.URL + "/status")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	for i := 0; i < 5; i++ {
		resp, err := http.Get(rig.rest.URL + "/cosmos/auth/v1beta1/params")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}
