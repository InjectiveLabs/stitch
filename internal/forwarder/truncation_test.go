package forwarder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/metrics"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/server"
	"github.com/InjectiveLabs/stitch/internal/types"
)

func TestStreamedTruncationAbortsHTTP1AndHTTP2(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		name := "HTTP1"
		if h2 {
			name = "HTTP2"
		}
		t.Run(name, func(t *testing.T) {
			prefix := []byte(strings.Repeat("x", 128<<10))
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/healthy" {
					_, _ = w.Write([]byte(`{"ok":true}`))
					return
				}
				_, _ = w.Write(prefix)
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			}))
			defer upstream.Close()
			var retryCalls atomic.Int32
			next := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				retryCalls.Add(1)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			defer next.Close()
			fwd := newForwarder(stubSelector{cands: []*backend.Backend{mkBackend("stream", upstream.URL), mkBackend("next", next.URL)}})
			front := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fwd.Forward(w, r, types.RouteKey{Protocol: types.ProtoRPC, Idempotent: true})
			}))
			front.EnableHTTP2 = h2
			front.StartTLS()
			defer front.Close()
			resp, err := front.Client().Get(front.URL + "/broken")
			if err != nil {
				t.Fatal(err) // The large prefix commits headers before the abort.
			}
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			wantProto := 1
			if h2 {
				wantProto = 2
			}
			if resp.ProtoMajor != wantProto || readErr == nil || len(body) == 0 {
				t.Fatalf("expected partial body plus transport error over %s: protocol=%s bytes=%d err=%v", name, resp.Proto, len(body), readErr)
			}
			if retryCalls.Load() != 0 {
				t.Fatal("must not append a retry to a committed response")
			}
			resp, err = front.Client().Get(front.URL + "/healthy")
			if err != nil {
				t.Fatal(err)
			}
			body, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil || string(body) != `{"ok":true}` {
				t.Fatalf("server unusable after abort: body=%q err=%v", body, err)
			}
		})
	}
}

func TestBufferedNonIdempotentTruncationDoesNotRetry(t *testing.T) {
	var nextCalls atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.Header().Set("Content-Encoding", "identity")
		_, _ = w.Write([]byte(`{"result":`))
	}))
	defer bad.Close()
	next := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		nextCalls.Add(1)
		_, _ = w.Write([]byte(`{"result":true}`))
	}))
	defer next.Close()
	fwd := newForwarder(stubSelector{cands: []*backend.Backend{mkBackend("write", bad.URL), mkBackend("next-write", next.URL)}})
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured := server.NewCapture(w.Header())
		fwd.Forward(captured, r, types.RouteKey{Protocol: types.ProtoRPC, Idempotent: false})
		captured.FlushTo(w)
	}))
	defer front.Close()
	resp, err := front.Client().Post(front.URL, "application/json", strings.NewReader(`{"method":"write"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusBadGateway || !json.Valid(body) || bytes.Contains(body, []byte(`"result"`)) {
		t.Fatalf("response: status=%d body=%q err=%v", resp.StatusCode, body, err)
	}
	if resp.Header.Get("Content-Encoding") != "" || nextCalls.Load() != 0 {
		t.Fatalf("stale encoding=%q retries=%d", resp.Header.Get("Content-Encoding"), nextCalls.Load())
	}
}

type notifyingCapture struct {
	*server.Capture
	written chan struct{}
}

func (c *notifyingCapture) Write(body []byte) (int, error) {
	n, err := c.Capture.Write(body)
	select {
	case c.written <- struct{}{}:
	default:
	}
	return n, err
}

func TestBufferedBodyCancellationReleasesCircuitAndStopsRetries(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"result":`))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer upstream.Close()
	var nextCalls atomic.Int32
	next := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		nextCalls.Add(1)
	}))
	defer next.Close()
	cm := circuit.NewManager(circuit.Policy{ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: time.Nanosecond})
	fwd := NewHTTP(stubSelector{cands: []*backend.Backend{mkBackend("canceled", upstream.URL), mkBackend("untried", next.URL)}}, pool.NewHTTPPool(), cm, Policy{MaxAttempts: 2, PerAttemptTimeout: time.Second})
	for attempt := range 3 {
		wantState := circuit.StateClosed
		if attempt == 2 {
			cm.Record("canceled", types.ProtoRPC, false)
			cm.Record("canceled", types.ProtoRPC, false)
			wantState = circuit.StateHalfOpen
		}
		ctx, cancel := context.WithCancel(context.Background())
		req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
		captured := &notifyingCapture{Capture: server.NewCapture(make(http.Header)), written: make(chan struct{}, 1)}
		done := make(chan struct{})
		go func() {
			defer close(done)
			fwd.Forward(captured, req, types.RouteKey{Protocol: types.ProtoRPC, Idempotent: true})
		}()
		select {
		case <-captured.written:
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatal("upstream body did not reach the capture")
		}
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("forwarder did not stop on cancellation")
		}
		if bytes.Contains(captured.BodyBytes(), []byte(`"result"`)) {
			t.Fatalf("canceled body survived: %q", captured.BodyBytes())
		}
		if cm.State("canceled", types.ProtoRPC) != wantState || nextCalls.Load() != 0 {
			t.Fatalf("cancellation changed circuit or retried: state=%s want=%s retries=%d", cm.State("canceled", types.ProtoRPC), wantState, nextCalls.Load())
		}
	}
	if !cm.Acquire("canceled", types.ProtoRPC) {
		t.Fatal("body cancellation did not release the half-open canary")
	}
	cm.Release("canceled", types.ProtoRPC)
}

func TestBufferedTruncationMetricsCountOnlyFinalOutcome(t *testing.T) {
	for _, attempts := range []int{1, 2} {
		t.Run(fmt.Sprint(attempts), func(t *testing.T) {
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", "1000")
				_, _ = w.Write([]byte(`{"result":`))
			}))
			defer bad.Close()
			good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"result":true}`))
			}))
			defer good.Close()
			badName, goodName := "metrics-bad-"+t.Name(), "metrics-good-"+t.Name()
			fwd := newForwarderWithCircuit(stubSelector{cands: []*backend.Backend{mkBackend(badName, bad.URL), mkBackend(goodName, good.URL)}}, newTestCircuit(), attempts)
			front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				captured := server.NewCapture(w.Header())
				fwd.Forward(captured, r, types.RouteKey{Protocol: types.ProtoRPC, Class: types.ClassLatest, Idempotent: true})
				captured.FlushTo(w)
			}))
			defer front.Close()
			counter := func(name, outcome string) float64 {
				return testutil.ToFloat64(metrics.RequestsTotal.WithLabelValues(string(types.ProtoRPC), types.ClassLatest.String(), name, outcome))
			}
			badBefore, goodBefore, failedBefore := counter(badName, "2xx"), counter(goodName, "2xx"), counter("-", "all_failed")
			badDurationBefore := requestDurationCount(t, string(types.ProtoRPC), types.ClassLatest.String(), badName)
			truncatedBefore := testutil.ToFloat64(metrics.RelayTruncated.WithLabelValues(badName, string(types.ProtoRPC)))
			resp, err := front.Client().Get(front.URL)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil || !json.Valid(body) {
				t.Fatalf("invalid buffered outcome: body=%q err=%v", body, err)
			}
			wantGood, wantFailed, wantStatus := float64(0), float64(1), http.StatusBadGateway
			if attempts == 2 {
				wantGood, wantFailed, wantStatus = 1, 0, http.StatusOK
			}
			if resp.StatusCode != wantStatus || counter(goodName, "2xx")-goodBefore != wantGood || counter("-", "all_failed")-failedBefore != wantFailed {
				t.Fatalf("final outcome must be counted exactly once: status=%d success=%v failed=%v", resp.StatusCode, counter(goodName, "2xx")-goodBefore, counter("-", "all_failed")-failedBefore)
			}
			if counter(badName, "2xx") != badBefore || requestDurationCount(t, string(types.ProtoRPC), types.ClassLatest.String(), badName) != badDurationBefore {
				t.Fatal("discarded buffered attempt counted as a completed response")
			}
			if got := testutil.ToFloat64(metrics.RelayTruncated.WithLabelValues(badName, string(types.ProtoRPC))) - truncatedBefore; got != 1 {
				t.Fatalf("truncated attempt metric delta=%v, want 1", got)
			}
		})
	}
}
