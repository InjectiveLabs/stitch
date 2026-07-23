package selector

import (
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// BenchmarkCandidates measures the cost of one selector decision over a
// realistic-sized backend fleet. Routing happens once per request, so
// this is on the request hot path.
func BenchmarkCandidates(b *testing.B) {
	for _, n := range []int{2, 8, 32} {
		b.Run("backends="+itoa(n), func(b *testing.B) {
			bs := make([]*backend.Backend, n)
			for i := range bs {
				bs[i] = &backend.Backend{
					Name:      "b" + itoa(i),
					Coverage:  backend.Coverage{Kind: backend.CovBounded, Lower: int64(i*1000) + 1, Upper: int64((i + 1) * 1000)},
					Weight:    100,
					Endpoints: map[types.Protocol]string{types.ProtoRPC: "http://x"},
				}
			}
			reg := backend.NewRegistry(bs)
			h := health.NewRegistry()
			for _, bb := range bs {
				h.Update(health.Snapshot{Backend: bb.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 1_000_000})
			}
			cm := circuit.NewManager(circuit.Policy{
				ErrorThreshold: 0.5, MinRequests: 4, OpenDuration: time.Second,
			})
			s := NewRangeSelector(reg, h, cm, 0)
			height := int64(500)
			key := types.RouteKey{
				Protocol: types.ProtoRPC,
				Method:   "block",
				Class:    types.ClassByHeight,
				Height:   &height,
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = s.Candidates(key)
			}
		})
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	out := []byte{}
	if i < 0 {
		out = append(out, '-')
		i = -i
	}
	digits := []byte{}
	for i > 0 {
		digits = append(digits, byte('0'+i%10))
		i /= 10
	}
	for j := len(digits) - 1; j >= 0; j-- {
		out = append(out, digits[j])
	}
	return string(out)
}
