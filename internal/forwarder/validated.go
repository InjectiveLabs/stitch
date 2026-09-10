package forwarder

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/config"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// ArchiveView keeps advertised history separate from currently usable replicas.
func (f *HTTP) ArchiveView() (config.ArchiveProfile, []*backend.Backend, int64, bool) {
	s, ok := f.selector.(interface {
		ArchiveProfile() (config.ArchiveProfile, bool)
		ArchiveUniverse(types.Protocol) []*backend.Backend
		ArchiveHead() int64
	})
	if !ok {
		return config.ArchiveProfile{}, nil, 0, false
	}
	p, enabled := s.ArchiveProfile()
	return p, s.ArchiveUniverse(types.ProtoRPC), s.ArchiveHead(), enabled
}

func (f *HTTP) Candidates(key types.RouteKey) []*backend.Backend {
	return f.selector.Candidates(key)
}

// ValidatedAttempt buffers an explicitly read-only reply before admitting it.
// Callers own method-specific validation and replica/search completeness.
func (f *HTTP) ValidatedAttempt(r *http.Request, b *backend.Backend, key types.RouteKey, body []byte, limit int64, validate func([]byte) error) ([]byte, error) {
	if !key.Idempotent || !b.Has(key.Protocol) {
		return nil, fmt.Errorf("validated replay requires a read-only endpoint")
	}
	if !f.circuit.Acquire(b.Name, key.Protocol) {
		return nil, fmt.Errorf("backend circuit is open")
	}
	// Historical block results are large. Keep an outer request deadline too.
	ctx, cancel := context.WithTimeout(r.Context(), max(f.policy.PerAttemptTimeout, 10*time.Second))
	defer cancel()
	address, err := buildUpstreamURL(b.Endpoint(key.Protocol), r.URL.Path, r.URL.RawQuery)
	var response []byte
	if err == nil {
		var req *http.Request
		req, err = http.NewRequestWithContext(ctx, r.Method, address, bytes.NewReader(body))
		if err == nil {
			copyHeaders(req.Header, r.Header)
			var resp *http.Response
			// The host is operator-configured; client input only supplies path/query.
			resp, err = f.pool.Client(b.Name, max(f.policy.PerAttemptTimeout, 10*time.Second)).Do(req) //nolint:gosec // G704: trusted backend endpoint.
			if err == nil {
				response, err = io.ReadAll(io.LimitReader(resp.Body, limit+1))
				_ = resp.Body.Close()
				if err == nil && int64(len(response)) > limit {
					err = fmt.Errorf("historical response exceeds %d bytes", limit)
				}
				if err == nil && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
					err = fmt.Errorf("upstream HTTP status %d", resp.StatusCode)
				}
				if err == nil {
					err = validate(response)
				}
			}
		}
	}
	if r.Context().Err() != nil {
		f.circuit.Release(b.Name, key.Protocol)
	} else {
		f.circuit.Record(b.Name, key.Protocol, err == nil)
	}
	if err != nil {
		return nil, err
	}
	return response, nil
}
