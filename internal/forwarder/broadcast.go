package forwarder

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/types"
)

// broadcastResult collects the per-candidate outcome of one Broadcast.
type broadcastResult struct {
	backend string
	resp    *http.Response
	body    []byte
	err     error
}

// Broadcast fans the request out to every healthy candidate in parallel
// and returns the first successful response to the client. Other
// in-flight responses are read to completion (so we can credit the
// circuit breakers) but discarded.
//
// Used for tx submission methods like broadcast_tx_*, eth_sendRawTransaction
// — the upstream mempool dedupes, so duplicate sends are harmless and
// removing the single-point-of-failure shape is worth the bandwidth.
func (f *HTTP) Broadcast(w http.ResponseWriter, r *http.Request, key types.RouteKey) {
	candidates := f.selector.Candidates(key)
	if len(candidates) == 0 {
		writeJSONError(w, http.StatusServiceUnavailable, "no eligible backend")
		metrics.BroadcastFanout.WithLabelValues("no_candidates").Inc()
		return
	}

	var bodyBytes []byte
	if r.Body != nil && r.ContentLength != 0 {
		var err error
		bodyBytes, err = io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "read body: "+err.Error())
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), f.policy.PerAttemptTimeout)
	defer cancel()

	resCh := make(chan broadcastResult, len(candidates))
	dispatched := 0

	for _, b := range candidates {
		ep := b.Endpoint(key.Protocol)
		if ep == "" {
			continue
		}
		// Acquire, not the read-only Allow: a half-open backend admits at
		// most one canary, and every dispatched goroutine's outcome is
		// recorded below, which releases the slot.
		if !f.circuit.Acquire(b.Name, key.Protocol) {
			continue
		}
		dispatched++
		go func(b *backend.Backend, ep string) {
			resCh <- f.broadcastOne(ctx, r, ep, b.Name, bodyBytes)
		}(b, ep)
	}

	if dispatched == 0 {
		writeJSONError(w, http.StatusServiceUnavailable, "all candidates blocked by circuit breaker")
		metrics.BroadcastFanout.WithLabelValues("all_circuited").Inc()
		return
	}

	// Wait for first success.
	var winner *broadcastResult
	failures := []broadcastResult{}
	for i := 0; i < dispatched; i++ {
		res := <-resCh
		if res.err == nil && res.resp != nil && res.resp.StatusCode < 500 {
			winner = &res
			f.circuit.Record(res.backend, key.Protocol, true)
			go f.drainResults(resCh, dispatched-i-1, key.Protocol)
			break
		}
		failures = append(failures, res)
		// A leg cancelled before any winner exists means the CLIENT went
		// away; that indicts nobody. Release the admission instead of
		// recording a failure — the same convention drainResults applies
		// to post-winner losers. Deadline expiry is NOT cancellation: a
		// timed-out leg still debits its backend below.
		if releaseOnCancel(r.Context(), ctx, res.err) {
			f.circuit.Release(res.backend, key.Protocol)
		} else {
			f.circuit.Record(res.backend, key.Protocol, false)
		}
	}

	if winner == nil {
		// Total failure — pick the most informative error to report.
		report := pickErrReport(failures)
		writeJSONError(w, http.StatusBadGateway, report)
		log.FromCtx(r.Context()).Error("broadcast: all candidates failed",
			"dispatched", dispatched,
			"failures", failureReports(failures),
			"err", report,
		)
		metrics.BroadcastFanout.WithLabelValues("total_failure").Inc()
		return
	}

	// Success!
	cancel()
	copyHeaders(w.Header(), winner.resp.Header)
	w.WriteHeader(winner.resp.StatusCode)
	_, _ = w.Write(winner.body)
	if len(failures) == 0 {
		metrics.BroadcastFanout.WithLabelValues("success").Inc()
	} else {
		metrics.BroadcastFanout.WithLabelValues("partial").Inc()
	}
	metrics.RequestsTotal.WithLabelValues(string(key.Protocol), key.Class.String(), winner.backend, statusBucket(winner.resp.StatusCode)).Inc()
}

// broadcastOne sends one upstream request and reads the body. The body
// read happens here so the response can be safely closed before the
// channel send.
func (f *HTTP) broadcastOne(ctx context.Context, orig *http.Request, ep, backendName string, body []byte) broadcastResult {
	out := broadcastResult{backend: backendName}

	url, err := buildUpstreamURL(ep, orig.URL.Path, orig.URL.RawQuery)
	if err != nil {
		out.err = err
		return out
	}
	req, err := http.NewRequestWithContext(ctx, orig.Method, url, bytes.NewReader(body))
	if err != nil {
		out.err = err
		return out
	}
	copyHeaders(req.Header, orig.Header)

	started := time.Now()
	client := f.pool.Client(backendName, f.policy.PerAttemptTimeout)
	resp, err := client.Do(req)
	dur := time.Since(started)
	metrics.BackendLatency.WithLabelValues(backendName, "broadcast").Observe(dur.Seconds())
	if err != nil {
		out.err = err
		return out
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		out.err = err
		return out
	}
	out.resp = resp
	out.body = bodyBytes
	return out
}

// drainResults consumes the remaining N results from resCh and resolves
// their admissions against the circuit breaker. Called in a goroutine
// after a winner has already been declared. A loser cancelled by the
// winner's cancel says nothing about its backend: its admission is
// released, not recorded — recording would re-trip a recovering backend
// and double its backoff. Legs that never dispatched have no admission to
// resolve.
func (f *HTTP) drainResults(resCh <-chan broadcastResult, n int, p types.Protocol) {
	for i := 0; i < n; i++ {
		res := <-resCh
		switch {
		case errors.Is(res.err, errHedgeLegSkipped):
		case errors.Is(res.err, context.Canceled):
			f.circuit.Release(res.backend, p)
		case res.err != nil || (res.resp != nil && res.resp.StatusCode >= 500):
			f.circuit.Record(res.backend, p, false)
		default:
			f.circuit.Record(res.backend, p, true)
		}
	}
}

func pickErrReport(failures []broadcastResult) string {
	if len(failures) == 0 {
		return "no upstream attempts"
	}
	// Prefer transport errors (most informative); fall back to first 5xx.
	for _, f := range failures {
		if f.err != nil && !errors.Is(f.err, context.Canceled) {
			return fmt.Sprintf("backend %s: %v", f.backend, f.err)
		}
	}
	for _, f := range failures {
		if f.err != nil {
			return fmt.Sprintf("backend %s: %v", f.backend, f.err)
		}
	}
	for _, f := range failures {
		if f.resp != nil {
			return fmt.Sprintf("backend %s: status %d", f.backend, f.resp.StatusCode)
		}
	}
	return "all upstream attempts failed"
}

func failureReports(failures []broadcastResult) []string {
	out := make([]string, 0, len(failures))
	for _, f := range failures {
		switch {
		case f.err != nil:
			out = append(out, fmt.Sprintf("%s: %v", f.backend, f.err))
		case f.resp != nil:
			out = append(out, fmt.Sprintf("%s: status %d", f.backend, f.resp.StatusCode))
		default:
			out = append(out, fmt.Sprintf("%s: no response", f.backend))
		}
	}
	return out
}

func releaseOnCancel(clientCtx, attemptCtx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) &&
		clientCtx.Err() != nil &&
		!errors.Is(attemptCtx.Err(), context.DeadlineExceeded)
}
