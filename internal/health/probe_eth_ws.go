package health

import (
	"context"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/decentrio/stitch/internal/backend"
)

// EthWSProber maintains a long-lived eth_subscribe newHeads stream per
// backend that has an eth_ws endpoint. Each header arrival is pushed into
// the health registry as a Protocol: ProtoEthWS snapshot. The registry's
// atomic MaxHead bumps automatically, giving the selector near-real-time
// head with no API changes elsewhere.
//
// Health for EVM HTTP routing continues to be sourced from the CometBFT
// /status poll via selector.healthProtocol(ProtoEthRPC) -> ProtoRPC.
// A dropped WS connection therefore degrades freshness only, not
// eligibility.
type EthWSProber struct {
	registry *backend.Registry
	health   *Registry
	dialer   *websocket.Dialer
	refresh  time.Duration // backend-set reconciliation tick
}

// NewEthWSProber returns a prober wired against the given registries.
func NewEthWSProber(reg *backend.Registry, h *Registry) *EthWSProber {
	return &EthWSProber{
		registry: reg,
		health:   h,
		dialer:   &websocket.Dialer{HandshakeTimeout: 5 * time.Second},
		refresh:  30 * time.Second,
	}
}

// Run blocks until ctx is cancelled. Real implementation lands in Task 5.
func (p *EthWSProber) Run(ctx context.Context) {
	<-ctx.Done()
}

// normalizeWS maps http(s):// to ws(s):// for endpoints configured as
// HTTP URLs; pass-through for already-ws URLs or unknown schemes.
func normalizeWS(ep string) string {
	switch {
	case strings.HasPrefix(ep, "ws://"), strings.HasPrefix(ep, "wss://"):
		return ep
	case strings.HasPrefix(ep, "http://"):
		return "ws://" + strings.TrimPrefix(ep, "http://")
	case strings.HasPrefix(ep, "https://"):
		return "wss://" + strings.TrimPrefix(ep, "https://")
	default:
		return ep
	}
}
