// Package forwarder owns the candidate-iteration loop that turns a routing
// decision into a successful upstream call (or, on exhaustion, a single
// aggregated error).
package forwarder

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/pool"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/types"
)

// Policy controls retry behavior.
type Policy struct {
	MaxAttempts       int
	PerAttemptTimeout time.Duration
	HedgeAfter        time.Duration // 0 disables hedging
}

// HTTP forwards an HTTP request by iterating selector candidates, applying
// circuit breaking and retry per Policy.
type HTTP struct {
	selector selector.Selector
	pool     *pool.HTTPPool
	circuit  *circuit.Manager
	policy   Policy
}

func NewHTTP(s selector.Selector, p *pool.HTTPPool, cm *circuit.Manager, pol Policy) *HTTP {
	if pol.MaxAttempts < 1 {
		pol.MaxAttempts = 1
	}
	if pol.PerAttemptTimeout <= 0 {
		pol.PerAttemptTimeout = 5 * time.Second
	}
	if pol.HedgeAfter <= 0 {
		pol.HedgeAfter = 200 * time.Millisecond
	}
	return &HTTP{selector: s, pool: p, circuit: cm, policy: pol}
}

// ErrNoCandidates means selector returned an empty list — no backend can
// serve the routing key (no protocol endpoint, all unhealthy, all tripped,
// or coverage gap).
var ErrNoCandidates = errors.New("no eligible backend candidates")

// ErrAllAttemptsFailed wraps the last attempt's error after exhaustion.
type ErrAllAttemptsFailed struct {
	Attempts int
	Last     error
}

func (e *ErrAllAttemptsFailed) Error() string {
	return fmt.Sprintf("all %d attempts failed: %v", e.Attempts, e.Last)
}

func (e *ErrAllAttemptsFailed) Unwrap() error { return e.Last }

// Forward proxies r to upstream, choosing an endpoint URL per candidate by
// joining the candidate's protocol-specific base with r.URL.Path + r.URL.RawQuery.
//
// The original r.Body is buffered once so that retries can replay it.
func (f *HTTP) Forward(w http.ResponseWriter, r *http.Request, key types.RouteKey) {
	candidates := f.selector.Candidates(key)
	if len(candidates) == 0 {
		writeJSONError(w, http.StatusServiceUnavailable, "no eligible backend")
		metrics.RequestsTotal.WithLabelValues(string(key.Protocol), key.Class.String(), "-", "no_candidates").Inc()
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

	var lastErr error
	attempts := 0
	for _, b := range candidates {
		if attempts >= f.policy.MaxAttempts {
			break
		}
		if !key.Idempotent && attempts > 0 {
			break
		}
		attempts++
		started := time.Now()

		ep := b.Endpoint(key.Protocol)
		if ep == "" {
			continue
		}
		upstreamURL, err := buildUpstreamURL(ep, r.URL.Path, r.URL.RawQuery)
		if err != nil {
			lastErr = err
			f.circuit.Record(b.Name, key.Protocol, false)
			continue
		}

		ctx, cancel := context.WithTimeout(r.Context(), f.policy.PerAttemptTimeout)
		req, err := http.NewRequestWithContext(ctx, r.Method, upstreamURL, bytes.NewReader(bodyBytes))
		if err != nil {
			cancel()
			lastErr = err
			f.circuit.Record(b.Name, key.Protocol, false)
			continue
		}
		copyHeaders(req.Header, r.Header)

		client := f.pool.Client(b.Name, f.policy.PerAttemptTimeout)
		resp, err := client.Do(req)
		dur := time.Since(started)
		metrics.BackendLatency.WithLabelValues(b.Name, string(key.Protocol)).Observe(dur.Seconds())

		if err != nil {
			cancel()
			lastErr = err
			f.circuit.Record(b.Name, key.Protocol, false)
			metrics.FailoverAttempts.WithLabelValues(b.Name, "next", classifyErr(err)).Inc()
			log.FromCtx(r.Context()).Warn("upstream attempt failed",
				"backend", b.Name,
				"protocol", string(key.Protocol),
				"err", err.Error(),
				"attempt", attempts,
			)
			continue
		}

		if shouldRetryStatus(resp.StatusCode, key) {
			lastErr = fmt.Errorf("upstream status %d", resp.StatusCode)
			_ = resp.Body.Close()
			cancel()
			f.circuit.Record(b.Name, key.Protocol, false)
			metrics.FailoverAttempts.WithLabelValues(b.Name, "next", "5xx").Inc()
			continue
		}

		// Success — copy through.
		copyHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		_ = resp.Body.Close()
		cancel()

		f.circuit.Record(b.Name, key.Protocol, true)
		metrics.RequestsTotal.WithLabelValues(string(key.Protocol), key.Class.String(), b.Name, statusBucket(resp.StatusCode)).Inc()
		metrics.RequestDuration.WithLabelValues(string(key.Protocol), key.Class.String(), b.Name).Observe(dur.Seconds())
		return
	}

	if lastErr == nil {
		lastErr = ErrNoCandidates
	}
	log.FromCtx(r.Context()).Error("all upstream attempts failed",
		"protocol", string(key.Protocol),
		"method", key.Method,
		"attempts", attempts,
		"err", lastErr.Error(),
	)
	writeJSONError(w, http.StatusBadGateway, lastErr.Error())
	metrics.RequestsTotal.WithLabelValues(string(key.Protocol), key.Class.String(), "-", "all_failed").Inc()
}

func buildUpstreamURL(base, path, rawQuery string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawQuery = rawQuery
	return u.String(), nil
}

func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		// Strip hop-by-hop headers per RFC 7230.
		switch strings.ToLower(k) {
		case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
			"te", "trailer", "transfer-encoding", "upgrade":
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

func shouldRetryStatus(status int, key types.RouteKey) bool {
	if !key.Idempotent {
		return false
	}
	switch status {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, 599:
		return true
	}
	return false
}

func statusBucket(code int) string {
	switch {
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 300 && code < 400:
		return "3xx"
	case code >= 400 && code < 500:
		return "4xx"
	case code >= 500:
		return "5xx"
	}
	return "other"
}

func classifyErr(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	return "transport"
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":` + jsonString(msg) + `}`))
}

func jsonString(s string) string {
	var sb strings.Builder
	sb.Grow(len(s) + 2)
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			sb.WriteByte('\\')
			sb.WriteRune(r)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			if r < 0x20 {
				sb.WriteString(fmt.Sprintf(`\u%04x`, r))
			} else {
				sb.WriteRune(r)
			}
		}
	}
	sb.WriteByte('"')
	return sb.String()
}

// Reference to keep the backend import surface visible to tooling that
// strips unused imports — backend.Backend is exported into the candidate
// list returned by the selector.
var _ = (*backend.Backend)(nil)
