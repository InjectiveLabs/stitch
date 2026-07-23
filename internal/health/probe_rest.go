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
	"github.com/InjectiveLabs/stitch/internal/types"
)

// RESTProber probes Cosmos REST `/cosmos/base/tendermint/v1beta1/blocks/latest`
// when no RPC endpoint exists for the backend. Most deployments have RPC
// alongside REST so this is a fallback.
type RESTProber struct {
	registry *backend.Registry
	health   *Registry
	interval time.Duration
	client   *http.Client
}

func NewRESTProber(reg *backend.Registry, h *Registry, interval time.Duration) *RESTProber {
	return &RESTProber{
		registry: reg,
		health:   h,
		interval: interval,
		client:   &http.Client{Timeout: 4 * time.Second},
	}
}

func (p *RESTProber) Run(ctx context.Context) {
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

func (p *RESTProber) tick(ctx context.Context) {
	for _, b := range p.registry.Snapshot() {
		// Bounded coverage is static — BoundedVerifier handles those once.
		if b.Coverage.Kind == backend.CovBounded {
			continue
		}
		// Only probe via REST if RPC is absent — saves work and keeps a single
		// source of truth per backend.
		if b.Endpoint(types.ProtoRPC) != "" {
			continue
		}
		ep := b.Endpoint(types.ProtoAPI)
		if ep == "" {
			continue
		}
		go p.probe(ctx, b.Name, ep)
	}
}

func (p *RESTProber) probe(ctx context.Context, name, base string) {
	url := strings.TrimRight(base, "/") + "/cosmos/base/tendermint/v1beta1/blocks/latest"
	start := time.Now()
	snap := Snapshot{
		Backend:   name,
		Protocol:  types.ProtoAPI,
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
		Block struct {
			Header struct {
				Height string `json:"height"`
			} `json:"header"`
		} `json:"block"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		snap.LastError = "decode: " + err.Error()
		return
	}
	h, err := strconv.ParseInt(body.Block.Header.Height, 10, 64)
	if err != nil {
		snap.LastError = "bad block.header.height: " + err.Error()
		return
	}
	snap.LatestHeight = h
	snap.Healthy = true
	if max := p.health.MaxHead(); max > 0 {
		snap.Lag = max - h
	}
}
