package eth_ws

import (
	"context"
	"errors"
	"time"

	"github.com/gorilla/websocket"

	"github.com/decentrio/stitch/internal/log"
)

const (
	pingInterval = 30 * time.Second
	pongWait     = 60 * time.Second
	writeWait    = 10 * time.Second
)

// relay runs two forwarding goroutines and blocks until either side errors
// or ctx is cancelled. Both connections are half-closed cleanly via close
// frames where possible.
//
// Per RFC 6455, only one writer per connection at a time is safe. Each
// connection has exactly one writing goroutine: client writer for messages
// flowing upstream→client, upstream writer for messages flowing
// client→upstream.
func relay(ctx context.Context, client, upstream *websocket.Conn) {
	// Configure pong handlers and read deadlines. We send pings to keep
	// each side alive; a pong from the peer extends the read deadline.
	configure(client)
	configure(upstream)

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)

	go pinger(rctx, client, "client")
	go pinger(rctx, upstream, "upstream")
	go forward(rctx, client, upstream, "c2u", errCh)
	go forward(rctx, upstream, client, "u2c", errCh)

	err := <-errCh
	cancel()

	closeCode, closeText := classifyClose(err)
	_ = client.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(closeCode, closeText),
		time.Now().Add(writeWait),
	)
	_ = upstream.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(closeCode, closeText),
		time.Now().Add(writeWait),
	)
}

func configure(c *websocket.Conn) {
	c.SetReadDeadline(time.Now().Add(pongWait))
	c.SetPongHandler(func(string) error {
		return c.SetReadDeadline(time.Now().Add(pongWait))
	})
	c.SetReadLimit(8 * 1024 * 1024)
}

// forward reads frames from src and writes them to dst. The first error
// pushes onto errCh and returns; the relay's owning goroutine then closes
// both connections.
func forward(ctx context.Context, src, dst *websocket.Conn, label string, errCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			errCh <- ctx.Err()
			return
		default:
		}
		mt, payload, err := src.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}
		if err := dst.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
			errCh <- err
			return
		}
		if err := dst.WriteMessage(mt, payload); err != nil {
			errCh <- err
			return
		}
		log.FromCtx(ctx).Debug("eth_ws: frame relayed", "dir", label, "type", mt, "bytes", len(payload))
	}
}

// pinger writes a ping every interval. The peer's pong handler extends
// the corresponding read deadline.
func pinger(ctx context.Context, c *websocket.Conn, label string) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				log.FromCtx(ctx).Debug("eth_ws: ping failed", "side", label, "err", err.Error())
				return
			}
		}
	}
}

// classifyClose maps a relay error to a WebSocket close code suitable for
// a control frame. We never expose internal errors verbatim.
func classifyClose(err error) (int, string) {
	if err == nil {
		return websocket.CloseNormalClosure, ""
	}
	if ce, ok := err.(*websocket.CloseError); ok {
		return ce.Code, ce.Text
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return websocket.CloseGoingAway, "shutdown"
	}
	return websocket.CloseInternalServerErr, "relay error"
}
