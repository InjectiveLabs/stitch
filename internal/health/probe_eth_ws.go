package health

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/log"
	"github.com/InjectiveLabs/stitch/internal/metrics"
	"github.com/InjectiveLabs/stitch/internal/types"
	"github.com/InjectiveLabs/stitch/internal/wsurl"
)

// EthWSProber maintains a long-lived eth_subscribe newHeads stream per
// backend that has an eth_ws endpoint. Each header arrival is pushed into
// the health registry as a Protocol: ProtoEthWS snapshot. The registry's
// atomic MaxHead bumps automatically, giving the selector near-real-time
// head with no API changes elsewhere.
//
// Health for EVM routing prefers the CometBFT /status poll (the ProtoRPC
// snapshot) when the backend has an rpc endpoint; for EVM-only backends
// the selector falls back to this prober's ProtoEthWS snapshot (see
// selector.healthProtocols). A dropped stream therefore degrades only
// freshness for dual-stack backends, but marks EVM-only backends
// unhealthy until the stream reconnects.
type EthWSProber struct {
	registry *backend.Registry
	health   *Registry
	dialer   *websocket.Dialer
	refresh  time.Duration // backend-set reconciliation tick

	// reconnect backoff knobs — exposed for tests to tighten the loop.
	baseBackoff            time.Duration
	maxBackoff             time.Duration
	healthyStreamThreshold time.Duration

	// kick requests an immediate reconciliation (buffered 1; coalesces).
	kick chan struct{}

	rand *rand.Rand
	mu   sync.Mutex // guards rand (rand.Rand is not goroutine-safe)
}

// NewEthWSProber returns a prober wired against the given registries.
func NewEthWSProber(reg *backend.Registry, h *Registry) *EthWSProber {
	return &EthWSProber{
		registry:               reg,
		health:                 h,
		dialer:                 &websocket.Dialer{HandshakeTimeout: wsHandshakeTimeout},
		refresh:                30 * time.Second,
		baseBackoff:            1 * time.Second,
		maxBackoff:             30 * time.Second,
		healthyStreamThreshold: 5 * time.Second,
		kick:                   make(chan struct{}, 1),
		rand:                   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Kick requests an immediate reconciliation of trackers against the
// backend registry — the reload path calls this right after registry.Set
// so trackers for removed backends are cancelled now rather than at the
// next refresh tick. Non-blocking; multiple kicks coalesce.
func (p *EthWSProber) Kick() {
	select {
	case p.kick <- struct{}{}:
	default:
	}
}

// Run blocks until ctx is cancelled, maintaining one trackOne goroutine
// per backend that has an eth_ws endpoint. Backends added or removed via
// registry.Set are picked up within p.refresh.
func (p *EthWSProber) Run(ctx context.Context) {
	type runner struct {
		cancel context.CancelFunc
		ep     string
	}
	runners := map[string]runner{}

	reconcile := func() {
		seen := map[string]bool{}
		for _, b := range p.registry.Snapshot() {
			// Bounded coverage is static — newHeads provides no routing
			// signal for backends that don't follow head. BoundedVerifier
			// handles their health once at startup.
			if b.Coverage.Kind == backend.CovBounded {
				continue
			}
			ep := b.Endpoint(types.ProtoEthWS)
			if ep == "" {
				continue
			}
			seen[b.Name] = true
			if r, ok := runners[b.Name]; ok {
				if r.ep == ep {
					continue
				}
				// Endpoint changed under us — restart the tracker.
				r.cancel()
			}
			bctx, cancel := context.WithCancel(ctx)
			runners[b.Name] = runner{cancel: cancel, ep: ep}
			name := b.Name
			go p.trackOne(bctx, name, ep)
		}
		for name, r := range runners {
			if !seen[name] {
				r.cancel()
				delete(runners, name)
			}
		}
	}

	reconcile()
	t := time.NewTicker(p.refresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			for _, r := range runners {
				r.cancel()
			}
			return
		case <-t.C:
			reconcile()
		case <-p.kick:
			reconcile()
		}
	}
}

// trackOne maintains a single backend's head subscription, reconnecting
// with exponential backoff on every dropped stream until ctx is cancelled.
// When a stream stays up longer than healthyStreamThreshold, the backoff
// resets — a flaky link that periodically reconnects gets baseBackoff
// per retry, not a runaway doubling.
func (p *EthWSProber) trackOne(ctx context.Context, name, ep string) {
	delay := time.Duration(0) // first attempt: immediate
	for {
		if ctx.Err() != nil {
			return
		}
		if delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
		start := time.Now()
		err := p.subscribeAndStream(ctx, name, ep)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errBackendRemoved) {
			// A hot reload pruned this backend. Reconcile will cancel our
			// ctx shortly anyway, but exiting now stops the per-block
			// snapshot/gauge resurrection — and we must not touch the
			// just-deleted BackendHealth child below.
			return
		}
		// Stream dropped — mark unhealthy for routing, not just dashboards:
		// write an unhealthy ProtoEthWS snapshot so selectors that fall back
		// to the WS witness (EVM-only backends with no CometBFT rpc endpoint)
		// stop routing here until the stream reconnects and heads flow again
		// (the per-frame Update in subscribeAndStream flips Healthy back).
		// Carry the last known height forward — MaxHead is monotonic either
		// way, but the truthful height keeps the snapshot useful.
		// Guarded on registry membership like the per-frame publish: after a
		// hot-reload prune this bookkeeping would resurrect the pruned
		// snapshot and gauge child in the window before reconcile cancels us.
		if p.registry.Has(name) {
			metrics.BackendHealth.WithLabelValues(name, string(types.ProtoEthWS)).Set(0)
			// Single-writer: only this backend's trackOne goroutine writes the
			// (name, ProtoEthWS) snapshot, so Get-then-Update is race-free.
			last, _ := p.health.Get(name, types.ProtoEthWS)
			p.health.Update(Snapshot{
				Backend:      name,
				Protocol:     types.ProtoEthWS,
				Healthy:      false,
				LatestHeight: last.LatestHeight,
				LastError:    errString(err),
				UpdatedAt:    time.Now(),
			})
		}
		log.L().Warn("eth_ws head stream dropped", "backend", name, "err", errString(err))
		if time.Since(start) >= p.healthyStreamThreshold {
			// Stream lasted long enough that we treat it as healthy —
			// reset to baseBackoff for the next attempt.
			delay = 0
		}
		delay = p.nextBackoff(delay)
	}
}

// nextBackoff doubles the previous delay (or starts at baseBackoff), then
// applies +/-20% jitter and clamps to [1ms, maxBackoff]. The 1ms floor
// prevents a hot spin if baseBackoff is somehow zero.
func (p *EthWSProber) nextBackoff(prev time.Duration) time.Duration {
	next := prev * 2
	if next < p.baseBackoff {
		next = p.baseBackoff
	}
	if next > p.maxBackoff {
		next = p.maxBackoff
	}
	// +/- 20% jitter around `next`.
	span := int64(next) / 5
	var result time.Duration
	if span <= 0 {
		result = next
	} else {
		p.mu.Lock()
		j := p.rand.Int63n(2*span) - span
		p.mu.Unlock()
		result = next + time.Duration(j)
	}
	// Final clamp so jitter can't exceed maxBackoff or drop below 1ms.
	if p.maxBackoff > 0 && result > p.maxBackoff {
		result = p.maxBackoff
	}
	if result < time.Millisecond {
		result = time.Millisecond
	}
	return result
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

const (
	wsReadDeadline     = 15 * time.Second
	wsWriteDeadline    = 5 * time.Second
	wsHandshakeTimeout = 5 * time.Second
	wsSubscribeRequest = `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`
)

// errBackendRemoved signals that the tracked backend disappeared from the
// registry (hot reload); trackOne exits instead of reconnecting.
var errBackendRemoved = errors.New("backend removed from registry")

// subscribeAndStream dials ep, sends eth_subscribe newHeads, then loops
// reading frames and pushing each header into the health registry. Returns
// on any I/O error or when ctx is cancelled.
func (p *EthWSProber) subscribeAndStream(ctx context.Context, name, ep string) error {
	wsURL := wsurl.Normalize(ep)
	conn, _, err := p.dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", wsURL, err)
	}
	defer conn.Close()

	// Cancel reads when ctx is done by closing the conn.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stop:
		}
	}()

	_ = conn.SetReadDeadline(time.Now().Add(wsReadDeadline))
	_ = conn.SetWriteDeadline(time.Now().Add(wsWriteDeadline))
	if err := conn.WriteMessage(websocket.TextMessage, []byte(wsSubscribeRequest)); err != nil {
		return fmt.Errorf("write subscribe: %w", err)
	}

	// Read subscribe ack. A JSON-RPC error response here aborts the loop;
	// outer caller reconnects with backoff.
	_, ack, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read ack: %w", err)
	}
	var ackResp struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(ack, &ackResp); err != nil {
		return fmt.Errorf("decode ack: %w", err)
	}
	if ackResp.Error != nil && string(ackResp.Error) != "null" {
		return fmt.Errorf("subscribe error: %s", ackResp.Error)
	}

	for {
		_ = conn.SetReadDeadline(time.Now().Add(wsReadDeadline))
		_, frame, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read frame: %w", err)
		}
		height, ok := parseNewHeadsNotification(frame)
		if !ok || height <= 0 {
			continue
		}
		// A hot reload may have pruned this backend; publishing would
		// resurrect its snapshot and gauges on every new head until the
		// next reconcile. Stop the tracker instead.
		if !p.registry.Has(name) {
			return errBackendRemoved
		}
		snap := Snapshot{
			Backend:      name,
			Protocol:     types.ProtoEthWS,
			LatestHeight: height,
			Healthy:      true,
			UpdatedAt:    time.Now(),
		}
		p.health.Update(snap)
		// Don't use emitHealth here: it would write 0 into BackendLagBlocks
		// (which is keyed only on backend name), clobbering the RPCProber's
		// computed lag. WS provides freshness; lag stays an RPCProber concern.
		// Still publish the per-protocol BackendHealth gauge so operators can
		// see whether the eth_ws stream is alive.
		metrics.BackendHealth.WithLabelValues(snap.Backend, string(snap.Protocol)).Set(1)
	}
}

// parseNewHeadsNotification extracts the block number from an
// eth_subscription newHeads notification frame. Returns (height, true) on
// success; (0, false) if the frame is not a matching notification.
//
// Inlined to avoid an import cycle: subscription -> selector -> health.
func parseNewHeadsNotification(raw []byte) (int64, bool) {
	var env struct {
		Method string `json:"method"`
		Params struct {
			Subscription string `json:"subscription"`
			Result       struct {
				Number string `json:"number"`
			} `json:"result"`
		} `json:"params"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(raw), &env); err != nil {
		return 0, false
	}
	if env.Method != "eth_subscription" || env.Params.Subscription == "" {
		return 0, false
	}
	s := strings.TrimSpace(env.Params.Result.Number)
	if s == "" {
		return 0, false
	}
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		h, err := strconv.ParseInt(s[2:], 16, 64)
		if err != nil {
			return 0, false
		}
		return h, true
	}
	h, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return h, true
}
