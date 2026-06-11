package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/types"
)

// BoundedVerifier validates each CovBounded backend once at startup.
//
// A bounded backend's coverage is static (`[Lower, Upper]`), so periodic
// head probes serve no routing purpose for it. Instead, we verify once
// that the backend actually contains the block at its declared Upper —
// either via CometBFT /status (latest_block_height >= Upper) or, when
// only an EVM endpoint is configured, eth_getBlockByNumber(Upper). On
// success the verifier writes a permanent "healthy" snapshot and exits;
// the RPC/REST/GRPC/EthWS probers skip bounded backends entirely.
//
// On failure the verifier retries with backoff a few times before marking
// the backend unhealthy. Operators see the misconfiguration in the admin
// API and the BackendHealth gauge.
type BoundedVerifier struct {
	registry *backend.Registry
	health   *Registry
	client   *http.Client

	maxAttempts int
	baseBackoff time.Duration
}

// NewBoundedVerifier returns a verifier wired against the given registries.
func NewBoundedVerifier(reg *backend.Registry, h *Registry) *BoundedVerifier {
	return &BoundedVerifier{
		registry:    reg,
		health:      h,
		client:      &http.Client{Timeout: 8 * time.Second},
		maxAttempts: 4,
		baseBackoff: 2 * time.Second,
	}
}

// Run verifies each bounded backend once, in parallel, then returns.
// Blocks until every per-backend attempt has finished (or ctx is cancelled).
func (v *BoundedVerifier) Run(ctx context.Context) {
	done := make(chan struct{})
	count := 0
	for _, b := range v.registry.Snapshot() {
		if b.Coverage.Kind != backend.CovBounded {
			continue
		}
		count++
		go func(b *backend.Backend) {
			defer func() { done <- struct{}{} }()
			v.verifyWithRetry(ctx, b)
		}(b)
	}
	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return
		case <-done:
		}
	}
}

// verifyWithRetry runs verify with exponential backoff up to maxAttempts.
func (v *BoundedVerifier) verifyWithRetry(ctx context.Context, b *backend.Backend) {
	delay := v.baseBackoff
	var lastErr error
	for attempt := 1; attempt <= v.maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return
		}
		err := v.verify(ctx, b)
		if err == nil {
			v.publish(b, true, "")
			log.L().Info("bounded backend verified",
				"backend", b.Name,
				"upper", b.Coverage.Upper,
				"attempt", attempt,
			)
			return
		}
		lastErr = err
		log.L().Warn("bounded backend verify failed",
			"backend", b.Name,
			"upper", b.Coverage.Upper,
			"attempt", attempt,
			"err", err.Error(),
		)
		if attempt == v.maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay *= 2
	}
	v.publish(b, false, lastErr.Error())
}

// verify performs one check against the backend. Prefers CometBFT /status
// (cheap, returns latest_block_height); falls back to eth_getBlockByNumber
// for backends with only EVM endpoints.
func (v *BoundedVerifier) verify(ctx context.Context, b *backend.Backend) error {
	upper := b.Coverage.Upper
	if ep := b.Endpoint(types.ProtoRPC); ep != "" {
		h, err := v.fetchCometStatus(ctx, ep)
		if err != nil {
			return err
		}
		if h < upper {
			return fmt.Errorf("backend latest_block_height=%d < declared upper %d", h, upper)
		}
		return nil
	}
	if ep := b.Endpoint(types.ProtoEthRPC); ep != "" {
		return v.fetchEthBlock(ctx, ep, upper)
	}
	return fmt.Errorf("no rpc or eth_rpc endpoint configured")
}

func (v *BoundedVerifier) fetchCometStatus(ctx context.Context, base string) (int64, error) {
	url := strings.TrimRight(base, "/") + "/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	var body struct {
		Result struct {
			SyncInfo struct {
				LatestBlockHeight string `json:"latest_block_height"`
			} `json:"sync_info"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, fmt.Errorf("decode: %w", err)
	}
	h, err := strconv.ParseInt(body.Result.SyncInfo.LatestBlockHeight, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad latest_block_height: %w", err)
	}
	return h, nil
}

func (v *BoundedVerifier) fetchEthBlock(ctx context.Context, ep string, height int64) error {
	body := fmt.Sprintf(
		`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x%x",false]}`,
		height,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	var rpcResp struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if rpcResp.Error != nil && string(rpcResp.Error) != "null" {
		return fmt.Errorf("rpc error: %s", rpcResp.Error)
	}
	if len(rpcResp.Result) == 0 || string(rpcResp.Result) == "null" {
		return fmt.Errorf("backend returned null for block 0x%x", height)
	}
	return nil
}

// publish writes a snapshot under ProtoRPC for the backend. The selector's
// healthProtocol maps every height-aware protocol onto ProtoRPC, so this
// single write satisfies eligibility lookups for ClassByHeight / ClassByHash
// regardless of which protocol the client used.
func (v *BoundedVerifier) publish(b *backend.Backend, healthy bool, lastError string) {
	// A hot reload may have pruned this backend while verification was in
	// flight (retries span tens of seconds); publishing now would resurrect
	// its snapshot and gauges.
	if !v.registry.Has(b.Name) {
		return
	}
	snap := Snapshot{
		Backend:      b.Name,
		Protocol:     types.ProtoRPC,
		Healthy:      healthy,
		LatestHeight: b.Coverage.Upper,
		UpdatedAt:    time.Now(),
		LastError:    lastError,
	}
	v.health.Update(snap)
	// Per-protocol BackendHealth gauge; deliberately do NOT call emitHealth
	// since that would also write BackendLagBlocks for this backend, and
	// bounded coverage's "lag" against MaxHead is structurally large and
	// not actionable.
	val := 0.0
	if healthy {
		val = 1
	}
	metrics.BackendHealth.WithLabelValues(b.Name, string(types.ProtoRPC)).Set(val)
}
