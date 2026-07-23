package forwarder

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// BenchmarkForwardE2E measures end-to-end forwarder cost against an
// instantly-replying upstream. Subtract the upstream's response latency
// (effectively zero for httptest in-process) and what's left is stitch
// added latency: candidate selection, circuit check, dial-or-pool,
// header copy, body relay.
func BenchmarkForwardE2E(b *testing.B) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	bk := &backend.Backend{
		Name:      "u",
		Coverage:  backend.Coverage{Kind: backend.CovArchive},
		Weight:    100,
		Endpoints: map[types.Protocol]string{types.ProtoRPC: upstream.URL},
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5, MinRequests: 4, OpenDuration: time.Second,
	})
	fwd := NewHTTP(stubSelector{cands: []*backend.Backend{bk}}, pool.NewHTTPPool(), cm, Policy{
		MaxAttempts:       1,
		PerAttemptTimeout: 2 * time.Second,
	})
	key := types.RouteKey{
		Protocol:   types.ProtoRPC,
		Method:     "status",
		Class:      types.ClassLatest,
		Idempotent: true,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rec := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/status", nil)
			fwd.Forward(rec, r, key)
		}
	})
}
