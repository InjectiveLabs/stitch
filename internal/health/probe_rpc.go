package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/log"
	"github.com/InjectiveLabs/stitch/internal/metrics"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// RPCProber probes CometBFT /status to learn each backend's latest height
// and reachability. One goroutine per backend.
type RPCProber struct {
	registry *backend.Registry
	health   *Registry
	interval time.Duration
	client   *http.Client
}

func NewRPCProber(reg *backend.Registry, h *Registry, interval time.Duration) *RPCProber {
	return &RPCProber{
		registry: reg,
		health:   h,
		interval: interval,
		client: &http.Client{
			Timeout: 4 * time.Second,
		},
	}
}

// Run blocks until ctx is cancelled, looping at interval and probing every
// backend that has an RPC endpoint.
func (p *RPCProber) Run(ctx context.Context) {
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

func (p *RPCProber) tick(ctx context.Context) {
	for _, b := range p.registry.Snapshot() {
		// Bounded coverage is static — BoundedVerifier handles those once at
		// startup; periodic /status polls would only update fields the
		// selector doesn't consult for bounded backends.
		if b.Coverage.Kind == backend.CovBounded {
			continue
		}
		ep := b.Endpoint(types.ProtoRPC)
		if ep == "" {
			continue
		}
		go p.probe(ctx, b.Name, ep)
	}
}

func (p *RPCProber) probe(ctx context.Context, name, base string) {
	url := strings.TrimRight(base, "/") + "/status"
	start := time.Now()
	snap := Snapshot{
		Backend:   name,
		Protocol:  types.ProtoRPC,
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		snap.LastError = err.Error()
		return
	}
	resp, err := p.client.Do(req)
	if err != nil {
		snap.LastError = err.Error()
		return
	}
	defer resp.Body.Close()
	snap.LatencyP50 = time.Since(start)

	if resp.StatusCode != http.StatusOK {
		snap.LastError = fmt.Sprintf("status %d", resp.StatusCode)
		return
	}
	var body struct {
		Result struct {
			SyncInfo struct {
				LatestBlockHeight string `json:"latest_block_height"`
				CatchingUp        bool   `json:"catching_up"`
			} `json:"sync_info"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		snap.LastError = "decode: " + err.Error()
		return
	}
	h, err := strconv.ParseInt(body.Result.SyncInfo.LatestBlockHeight, 10, 64)
	if err != nil {
		snap.LastError = "bad latest_block_height: " + err.Error()
		return
	}
	snap.LatestHeight = h
	snap.Healthy = !body.Result.SyncInfo.CatchingUp
	if max := p.health.MaxHead(); max > 0 {
		snap.Lag = max - h
	}
	log.L().Debug("probe ok", "backend", name, "protocol", "rpc", "height", h, "lag", snap.Lag)
}

func emitHealth(s Snapshot) {
	v := 0.0
	if s.Healthy {
		v = 1
	}
	metrics.BackendHealth.WithLabelValues(s.Backend, string(s.Protocol)).Set(v)
	metrics.BackendLagBlocks.WithLabelValues(s.Backend).Set(float64(s.Lag))
}
