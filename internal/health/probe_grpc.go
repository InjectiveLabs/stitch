package health

import (
	"context"
	"time"

	"google.golang.org/grpc/connectivity"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/pool"
	"github.com/decentrio/stitch/internal/types"
)

// GRPCProber checks gRPC connectivity to every backend that has a grpc
// endpoint. It does not query height (that's the RPC prober's job for the
// chain-wide signal); it only certifies reachability + handshake.
type GRPCProber struct {
	registry *backend.Registry
	pool     *pool.GRPCPool
	health   *Registry
	interval time.Duration
}

func NewGRPCProber(reg *backend.Registry, p *pool.GRPCPool, h *Registry, interval time.Duration) *GRPCProber {
	return &GRPCProber{registry: reg, pool: p, health: h, interval: interval}
}

func (p *GRPCProber) Run(ctx context.Context) {
	t := time.NewTicker(p.interval)
	defer t.Stop()
	p.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

func (p *GRPCProber) tick(ctx context.Context) {
	for _, b := range p.registry.Snapshot() {
		// Bounded coverage is static — BoundedVerifier handles those once.
		if b.Coverage.Kind == backend.CovBounded {
			continue
		}
		ep := b.Endpoint(types.ProtoGRPC)
		if ep == "" {
			continue
		}
		go p.probe(ctx, b.Name, pool.CleanAddr(ep))
	}
}

func (p *GRPCProber) probe(ctx context.Context, name, addr string) {
	snap := Snapshot{
		Backend:   name,
		Protocol:  types.ProtoGRPC,
		UpdatedAt: time.Now(),
	}
	defer func() {
		// A hot reload may have pruned this backend while the probe was in
		// flight; publishing now would resurrect its snapshot and gauges.
		if !p.registry.Has(name) {
			return
		}
		p.health.Update(snap)
		emitHealth(snap)
	}()

	dialCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	start := time.Now()
	conn, err := p.pool.Conn(dialCtx, name, addr)
	snap.LatencyP50 = time.Since(start)
	if err != nil {
		snap.LastError = err.Error()
		return
	}
	st := conn.GetState()
	switch st {
	case connectivity.Idle, connectivity.Ready, connectivity.Connecting:
		snap.Healthy = true
	default:
		snap.LastError = "conn state " + st.String()
		return
	}

	// Borrow latest height from the RPC prober so the lag computation works
	// uniformly even when only gRPC is configured for a given backend.
	if rpc, ok := p.health.Get(name, types.ProtoRPC); ok && rpc.LatestHeight > 0 {
		snap.LatestHeight = rpc.LatestHeight
		snap.Lag = rpc.Lag
	}
	// Same prune guard as the deferred publish: don't re-create the latency
	// summary child for a backend that was just removed.
	if p.registry.Has(name) {
		metrics.BackendLatency.WithLabelValues(name, string(types.ProtoGRPC)).Observe(snap.LatencyP50.Seconds())
	}
}
