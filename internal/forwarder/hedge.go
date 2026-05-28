package forwarder

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/types"
)

// Hedge dispatches the request to the top selector candidate immediately
// and to the second candidate after policy.HedgeAfter. The first
// successful response wins; the loser is drained in the background to
// keep the circuit-breaker accounting honest.
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

	ctx, cancel := context.WithTimeout(r.Context(), f.policy.PerAttemptTimeout)
	defer cancel()

	resCh := make(chan broadcastResult, 2)
	primary := candidates[0]
	secondary := candidates[1]

	// Primary fires immediately.
	go func() {
		ep := primary.Endpoint(key.Protocol)
		if ep == "" {
			resCh <- broadcastResult{backend: primary.Name, err: errors.New("primary has no endpoint for protocol")}
			return
		}
		resCh <- f.broadcastOne(ctx, r, ep, primary.Name, bodyBytes)
	}()

	// Secondary fires after hedgeAfter, unless ctx is cancelled first.
	go func() {
		select {
		case <-time.After(f.policy.HedgeAfter):
		case <-ctx.Done():
			resCh <- broadcastResult{backend: secondary.Name, err: ctx.Err()}
			return
		}
		ep := secondary.Endpoint(key.Protocol)
		if ep == "" {
			resCh <- broadcastResult{backend: secondary.Name, err: errors.New("secondary has no endpoint for protocol")}
			return
		}
		resCh <- f.broadcastOne(ctx, r, ep, secondary.Name, bodyBytes)
	}()

	// Wait for first success (or both failures).
	failures := []broadcastResult{}
	for i := 0; i < 2; i++ {
		res := <-resCh
		if res.err == nil && res.resp != nil && res.resp.StatusCode < 500 {
			cancel()
			f.circuit.Record(res.backend, key.Protocol, true)
			idx := "0"
			if res.backend == secondary.Name {
				idx = "1"
			}
			metrics.HedgeWins.WithLabelValues(key.Method, idx).Inc()
			copyHeaders(w.Header(), res.resp.Header)
			w.WriteHeader(res.resp.StatusCode)
			_, _ = w.Write(res.body)
			// Drain the loser asynchronously so its outcome credits the circuit.
			go f.drainResults(resCh, 1-i, key.Protocol)
			return
		}
		failures = append(failures, res)
		if res.err == nil || !errors.Is(res.err, context.Canceled) {
			f.circuit.Record(res.backend, key.Protocol, false)
		}
	}

	log.FromCtx(r.Context()).Error("hedge: both candidates failed",
		"protocol", string(key.Protocol),
		"method", key.Method,
		"primary", primary.Name,
		"secondary", secondary.Name,
		"err", pickErrReport(failures),
	)
	writeJSONError(w, http.StatusBadGateway, pickErrReport(failures))
}
