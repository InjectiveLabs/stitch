package subscription

import (
	"context"
	"time"

	"github.com/gorilla/websocket"

	"github.com/InjectiveLabs/stitch/internal/log"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// connTuning groups the upstream-connection timing knobs shared by engine
// sessions and hub upstreams. Production code always runs the defaults;
// package tests tighten them to keep wall time low.
//
// Liveness contract: readDeadline is refreshed by every pong AND by every
// successful read (engine upstreamReader, hub runUntilFailure — mirroring
// health/probe_eth_ws.go's per-frame refresh), so a peer that streams data
// but never answers pings is not churned at the read deadline.
type connTuning struct {
	dialTimeout   time.Duration // per-candidate WS dial budget
	readDeadline  time.Duration // max upstream quiet time; refreshed by pongs and data
	pingInterval  time.Duration // upstream keepalive ping cadence
	pingWriteWait time.Duration // write budget per ping control frame
}

// defaultConnTuning mirrors the health prober's eth_ws constants where the
// semantics line up (5s handshake/write); the read deadline stays at the
// historical 60s because subscription streams may legitimately idle far
// longer than a head probe.
func defaultConnTuning() connTuning {
	return connTuning{
		dialTimeout:   5 * time.Second,
		readDeadline:  60 * time.Second,
		pingInterval:  20 * time.Second,
		pingWriteWait: 5 * time.Second,
	}
}

// Resume re-dial backoff: first retry after resumeBackoffMin, doubling per
// failed pass, capped at resumeBackoffMax. The pass itself walks every
// selector candidate, so the backoff paces whole passes, not single dials.
const (
	resumeBackoffMin = 250 * time.Millisecond
	resumeBackoffMax = 2 * time.Second
)

// dialFirstCandidate walks the selector candidates for key and opens a WS
// to the first one that answers: normalize maps the configured endpoint to
// a dialable ws(s) URL, the dial is bounded by tune.dialTimeout, and the
// returned conn carries tune.readDeadline refreshed by pongs (pair it with
// keepAliveLoop so quiet streams keep producing pongs). Returns the conn
// and the backend name, or ok=false when every candidate fails. Connected
// logging stays with the caller — only per-candidate failures are logged
// here, under the caller's tag.
func dialFirstCandidate(ctx context.Context, sel selector.Selector, dialer *websocket.Dialer, key types.RouteKey, normalize func(string) string, tag string, tune connTuning) (*websocket.Conn, string, bool) {
	for _, b := range sel.Candidates(key) {
		ep := b.Endpoint(key.Protocol)
		if ep == "" {
			continue
		}
		addr := normalize(ep)
		dialCtx, cancel := context.WithTimeout(ctx, tune.dialTimeout)
		conn, _, err := dialer.DialContext(dialCtx, addr, nil)
		cancel()
		if err != nil {
			log.FromCtx(ctx).Warn(tag+": upstream dial failed", "backend", b.Name, "err", err.Error())
			continue
		}
		_ = conn.SetReadDeadline(time.Now().Add(tune.readDeadline))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(tune.readDeadline))
		})
		return conn, b.Name, true
	}
	return nil, "", false
}

// keepAliveLoop pings conn every tune.pingInterval until stop closes. The
// peer's pongs refresh the read deadline via the handler installed by
// dialFirstCandidate; without pings a quiet-but-healthy stream would hit
// the read deadline and churn through a needless resume. WriteControl is
// documented safe to call concurrently with WriteMessage, so the loop
// needs no coordination with the data writer. A failed ping closes the
// conn so the blocked reader fails over now instead of at the deadline.
func keepAliveLoop(conn *websocket.Conn, tune connTuning, stop <-chan struct{}) {
	t := time.NewTicker(tune.pingInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(tune.pingWriteWait)); err != nil {
				_ = conn.Close()
				return
			}
		}
	}
}
