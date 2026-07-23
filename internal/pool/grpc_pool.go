package pool

import (
	"context"
	"crypto/tls"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/InjectiveLabs/stitch/internal/log"
)

// GRPCPool maintains one *grpc.ClientConn per backend, with idle eviction —
// the gap that decentrio/gateway never closed. Connections are dialed
// lazily, refreshed transparently if the underlying TCP dies, and released
// after IdleTimeout of inactivity.
type GRPCPool struct {
	mu          sync.Mutex
	conns       map[string]*entry
	idleTimeout time.Duration
}

type entry struct {
	conn     *grpc.ClientConn
	addr     string
	lastUsed time.Time
}

// NewGRPCPool creates a pool with the given idle eviction timeout. Pass 0
// to disable eviction.
func NewGRPCPool(idleTimeout time.Duration) *GRPCPool {
	return &GRPCPool{
		conns:       make(map[string]*entry),
		idleTimeout: idleTimeout,
	}
}

// Conn returns a client connection for the given (backend, address). It
// (re)dials if the existing conn is closed. Every call marks the entry
// just-used for idle eviction — callers need no separate touch.
func (p *GRPCPool) Conn(ctx context.Context, backend, addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if e, ok := p.conns[backend]; ok && e.addr == addr {
		// Reuse healthy conn.
		if connOK(e.conn) {
			e.lastUsed = time.Now()
			return e.conn, nil
		}
		// Stale; tear down and redial.
		_ = e.conn.Close()
		delete(p.conns, backend)
	}

	conn, err := dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	p.conns[backend] = &entry{conn: conn, addr: addr, lastUsed: time.Now()}
	return conn, nil
}

// CloseAll tears down every cached connection. Called at shutdown.
func (p *GRPCPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for name, e := range p.conns {
		_ = e.conn.Close()
		delete(p.conns, name)
	}
}

// EvictIdle closes connections that have been unused for at least the
// configured idle timeout. Safe to call from a ticker goroutine.
func (p *GRPCPool) EvictIdle() {
	if p.idleTimeout <= 0 {
		return
	}
	cutoff := time.Now().Add(-p.idleTimeout)
	p.mu.Lock()
	defer p.mu.Unlock()
	for name, e := range p.conns {
		if e.lastUsed.Before(cutoff) {
			log.L().Debug("grpc pool: evicting idle conn", "backend", name, "addr", e.addr, "idle_for", time.Since(e.lastUsed).String())
			_ = e.conn.Close()
			delete(p.conns, name)
		}
	}
}

// RunEvictor blocks until ctx is cancelled, calling EvictIdle on a tick.
func (p *GRPCPool) RunEvictor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.EvictIdle()
		}
	}
}

func connOK(c *grpc.ClientConn) bool {
	if c == nil {
		return false
	}
	st := c.GetState().String()
	return st != "SHUTDOWN" && st != "TRANSIENT_FAILURE"
}

func dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024),
			grpc.MaxCallSendMsgSize(64*1024*1024),
		),
	}
	if useTLS(addr) {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return grpc.DialContext(dialCtx, addr, opts...)
}

// useTLS picks transport security based on a small heuristic: anything on
// :443 or with an explicit scheme prefix uses TLS. Operators can pre-pin
// the prefix in config (e.g. "tls://node:9900") if they need it.
func useTLS(addr string) bool {
	if strings.HasPrefix(addr, "tls://") || strings.HasPrefix(addr, "grpcs://") {
		return true
	}
	if strings.HasSuffix(addr, ":443") {
		return true
	}
	return false
}

// CleanAddr strips schemes used to mark TLS so the dialer sees a bare
// host:port. Callers should pass the result to Conn().
func CleanAddr(addr string) string {
	for _, prefix := range []string{"tls://", "grpcs://", "grpc://"} {
		if strings.HasPrefix(addr, prefix) {
			return strings.TrimPrefix(addr, prefix)
		}
	}
	return addr
}
