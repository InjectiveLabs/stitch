package forwarder

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/types"
)

// errHedgeLegSkipped marks a hedge leg that was never dispatched — breaker
// refused admission, no candidate left, or the race ended first. Nothing
// was acquired for such a leg, so its result must neither Record nor
// Release.
var errHedgeLegSkipped = errors.New("hedge leg not dispatched")

// Hedge dispatches the request to the first acquirable candidate
// immediately and to a second candidate after policy.HedgeAfter. The first
// successful response wins; the loser is drained in the background to
// keep the circuit-breaker accounting honest.
//
// Each leg is admitted via Acquire immediately before it fires: the
// primary at dispatch (falling back through the candidate list; with no
// acquirable primary, Forward owns the exhaustion path), the secondary at
// timer-fire (an unacquirable secondary simply never fires and the
// primary continues alone). Every dispatched leg resolves its admission
// with exactly one Record or Release.
//
// Falls back to the regular Forward path when fewer than 2 candidates
// are available — there's no second backend to hedge against.
//
// Hedging is only safe for idempotent reads. Callers must gate on the
// per-method manifest's hedge: true flag.
func (f *HTTP) Hedge(w http.ResponseWriter, r *http.Request, key types.RouteKey) {
	candidates := f.selector.Candidates(key)
	if len(candidates) < 2 {
		f.Forward(w, r, key)
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

	// Admit the primary: breakers may have tripped between selection and
	// now. Candidates without an endpoint or admission are skipped.
	var (
		primary   *backend.Backend
		primaryEp string
		next      int
	)
	for i, b := range candidates {
		ep := b.Endpoint(key.Protocol)
		if ep == "" {
			continue
		}
		if !f.circuit.Acquire(b.Name, key.Protocol) {
			continue
		}
		primary, primaryEp, next = b, ep, i+1
		break
	}
	if primary == nil {
		f.Forward(w, r, key)
		return
	}

	// Secondary candidate; its admission is checked at timer-fire.
	var (
		secondary   *backend.Backend
		secondaryEp string
	)
	for _, b := range candidates[next:] {
		if ep := b.Endpoint(key.Protocol); ep != "" {
			secondary, secondaryEp = b, ep
			break
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), f.policy.PerAttemptTimeout)
	defer cancel()

	resCh := make(chan broadcastResult, 2)

	// Primary fires immediately; its admission is already held.
	go func() {
		resCh <- f.broadcastOne(ctx, r, primaryEp, primary.Name, bodyBytes)
	}()

	// Secondary fires after HedgeAfter, unless the race ends first or the
	// breaker refuses admission.
	go func() {
		select {
		case <-time.After(f.policy.HedgeAfter):
		case <-ctx.Done():
			resCh <- broadcastResult{err: errHedgeLegSkipped}
			return
		}
		if secondary == nil || !f.circuit.Acquire(secondary.Name, key.Protocol) {
			resCh <- broadcastResult{err: errHedgeLegSkipped}
			return
		}
		resCh <- f.broadcastOne(ctx, r, secondaryEp, secondary.Name, bodyBytes)
	}()

	// Wait for first success (or both legs resolved).
	failures := []broadcastResult{}
	for i := 0; i < 2; i++ {
		res := <-resCh
		if res.err == nil && res.resp != nil && res.resp.StatusCode < 500 {
			cancel()
			f.circuit.Record(res.backend, key.Protocol, true)
			idx := "0"
			if secondary != nil && res.backend == secondary.Name {
				idx = "1"
			}
			metrics.HedgeWins.WithLabelValues(key.Method, idx).Inc()
			copyHeaders(w.Header(), res.resp.Header)
			w.WriteHeader(res.resp.StatusCode)
			_, _ = w.Write(res.body)
			// Drain the loser asynchronously so its admission is resolved.
			go f.drainResults(resCh, 1-i, key.Protocol)
			return
		}
		if errors.Is(res.err, errHedgeLegSkipped) {
			// Never dispatched: no admission to resolve, nothing to report.
			continue
		}
		failures = append(failures, res)
		if errors.Is(res.err, context.Canceled) {
			// Cancelled mid-flight: the outcome says nothing about the
			// backend, so free the admission claimed at dispatch.
			f.circuit.Release(res.backend, key.Protocol)
		} else {
			f.circuit.Record(res.backend, key.Protocol, false)
		}
	}

	secondaryName := "-"
	if secondary != nil {
		secondaryName = secondary.Name
	}
	log.FromCtx(r.Context()).Error("hedge: both candidates failed",
		"protocol", string(key.Protocol),
		"method", key.Method,
		"primary", primary.Name,
		"secondary", secondaryName,
		"err", pickErrReport(failures),
	)
	writeJSONError(w, http.StatusBadGateway, pickErrReport(failures))
}
