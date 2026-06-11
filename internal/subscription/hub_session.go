package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/types"
)

// HubSession serves one /injstream-ws client in multicast mode: subscribe
// and unsubscribe route through the shared Hub, which coalesces identical
// canonical filters onto one upstream connection per filter.
//
// Contract differences from the 1:1 InjSession:
//
//   - The subscribe ack ("success") is sent at attach time by this layer;
//     the Hub absorbs the upstream's acks (hub.go handleFrame), so this is
//     where the "per-client ack sent at attach time" promise is kept.
//   - Any other JSON-RPC frame is answered with a -32601 error instead of
//     per-session passthrough: multicast mode has no per-client upstream,
//     and opening one per client would defeat the mode. /injstream-ws
//     traffic is subscribe/unsubscribe in practice.
//   - When the Hub gives up on an upstream (no dialable backend within the
//     replay window), the affected subscriber's Done fires and the whole
//     client session terminates — mirroring the 1:1 session, which also
//     ends when its upstream is unrecoverable.
type HubSession struct {
	client *websocket.Conn
	hub    *Hub

	writeMu sync.Mutex
	closed  atomic.Bool

	mu   sync.Mutex
	subs map[string]*Subscriber // subscription_id → attached hub subscriber
}

// NewHubSession wires a client connection to the shared hub.
func NewHubSession(client *websocket.Conn, hub *Hub) *HubSession {
	return &HubSession{
		client: client,
		hub:    hub,
		subs:   make(map[string]*Subscriber),
	}
}

// Run blocks until the session terminates and returns the terminal cause.
// Termination paths: client read error (disconnect or server sweep), ctx
// cancel, or a hub-side subscription death (pump closes the client conn,
// which surfaces here as a read error).
func (s *HubSession) Run(ctx context.Context) error {
	proto := string(types.ProtoChainStream)
	metrics.SubscriptionsActive.WithLabelValues(proto, "inj_ws").Inc()
	defer metrics.SubscriptionsActive.WithLabelValues(proto, "inj_ws").Dec()
	defer s.teardown()

	// ReadMessage doesn't watch ctx; close the conn on cancel so it
	// unblocks (same pattern as the hub's per-conn watcher).
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			s.closeClient(websocket.CloseGoingAway, "session ending")
		case <-stop:
		}
	}()

	for {
		_, msg, err := s.client.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if err := s.handleClientFrame(ctx, msg); err != nil {
			return err
		}
	}
}

// handleClientFrame routes one client frame. Only the returned error of a
// client WRITE terminates the session; protocol-level rejections are
// answered in-band.
func (s *HubSession) handleClientFrame(ctx context.Context, msg []byte) error {
	var probe struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(msg, &probe); err != nil {
		// The 1:1 session forwards unparseable frames upstream; multicast
		// has no upstream to delegate the error to.
		return s.replyError(nil, -32700, "parse error")
	}
	switch probe.Method {
	case "subscribe":
		return s.handleSubscribe(ctx, probe.ID, probe.Params)
	case "unsubscribe":
		return s.handleUnsubscribe(probe.ID, probe.Params)
	default:
		return s.replyError(probe.ID, -32601, "multicast mode: only subscribe/unsubscribe are served on /injstream-ws")
	}
}

// handleSubscribe attaches the client to the hub upstream for the
// filter's canonical key and sends the attach-time ack. A re-subscribe
// under an existing subscription_id replaces the old attachment, like
// the 1:1 adapter.
func (s *HubSession) handleSubscribe(ctx context.Context, clientID, params json.RawMessage) error {
	sp, ok := ParseInjSubscribeParams(params)
	if !ok {
		return s.replyError(clientID, -32602, "subscribe params: missing subscription_id or filter")
	}
	sub, err := s.hub.Subscribe(ctx, sp.Filter, sp.SubscriptionID, clientID)
	if err != nil {
		if errors.Is(err, ErrHubClosed) {
			return s.replyError(clientID, -32603, "shutting down")
		}
		return s.replyError(clientID, -32602, "subscribe filter: "+err.Error())
	}

	s.mu.Lock()
	old := s.subs[sp.SubscriptionID] // replacing disowns it; its pump exits quietly
	s.subs[sp.SubscriptionID] = sub
	s.mu.Unlock()
	if old != nil {
		old.Detach()
	}

	// Attach-time ack: the hub absorbed (or will absorb) the upstream ack.
	if err := s.clientWrite(mustMarshalReply(clientID, "success")); err != nil {
		return err
	}
	go s.pump(sub, sp.SubscriptionID)
	return nil
}

// handleUnsubscribe detaches the named subscription and replies
// "success" — the 1:1 adapter replies success regardless of whether the
// id was live, and this layer keeps that contract.
func (s *HubSession) handleUnsubscribe(clientID, params json.RawMessage) error {
	up, ok := ParseInjUnsubscribeParams(params)
	if !ok {
		return s.replyError(clientID, -32602, "unsubscribe params: missing subscription_id")
	}
	s.mu.Lock()
	sub := s.subs[up.SubscriptionID]
	delete(s.subs, up.SubscriptionID) // disown BEFORE Detach so the pump exits quietly
	s.mu.Unlock()
	if sub != nil {
		sub.Detach()
	}
	return s.clientWrite(mustMarshalReply(clientID, "success"))
}

// pump forwards hub fan-out to the client until the subscriber dies.
// Done with the sub still owned in s.subs means the HUB ended it
// (upstream unrecoverable, slow-consumer disconnect): terminate the
// session by closing the client conn — Run's reader surfaces the close.
// Done after the session disowned it (unsubscribe, replace, teardown) is
// a quiet exit.
func (s *HubSession) pump(sub *Subscriber, subscriptionID string) {
	for {
		select {
		case msg := <-sub.Out:
			if err := s.clientWrite(msg); err != nil {
				sub.Detach()
				s.closeClient(websocket.CloseGoingAway, "client write failed")
				return
			}
		case <-sub.Done():
			s.mu.Lock()
			owned := s.subs[subscriptionID] == sub
			if owned {
				delete(s.subs, subscriptionID)
			}
			s.mu.Unlock()
			if owned {
				s.closeClient(websocket.CloseTryAgainLater, "upstream subscription lost")
			}
			return
		}
	}
}

// teardown disowns and detaches every subscriber, then closes the client.
func (s *HubSession) teardown() {
	s.mu.Lock()
	subs := make([]*Subscriber, 0, len(s.subs))
	for id, sub := range s.subs {
		subs = append(subs, sub)
		delete(s.subs, id)
	}
	s.mu.Unlock()
	for _, sub := range subs {
		sub.Detach()
	}
	s.closeClient(websocket.CloseGoingAway, "session ending")
}

// closeClient writes a close frame and closes the underlying conn once.
func (s *HubSession) closeClient(code int, msg string) {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.writeMu.Lock()
	_ = s.client.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(code, msg),
		time.Now().Add(2*time.Second),
	)
	s.writeMu.Unlock()
	_ = s.client.Close()
}

// clientWrite is serialized — the reply path and N pump goroutines share
// the conn, and gorilla forbids concurrent writers.
func (s *HubSession) clientWrite(msg []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.client.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return s.client.WriteMessage(websocket.TextMessage, msg)
}

func (s *HubSession) replyError(id json.RawMessage, code int, msg string) error {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": code, "message": msg},
	})
	return s.clientWrite(out)
}

func mustMarshalReply(id json.RawMessage, result string) []byte {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
	return out
}
