// Package pool caches HTTP transports per backend so connection reuse spans
// requests. One transport per backend prevents head-of-line blocking from
// other backends sharing an idle pool.
package pool

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// HTTPPool caches *http.Transport keyed by backend name.
type HTTPPool struct {
	mu         sync.RWMutex
	transports map[string]*http.Transport
}

func NewHTTPPool() *HTTPPool {
	return &HTTPPool{transports: make(map[string]*http.Transport)}
}

// Transport returns a backend-scoped transport; creates one on first use.
func (p *HTTPPool) Transport(backend string) *http.Transport {
	p.mu.RLock()
	t, ok := p.transports[backend]
	p.mu.RUnlock()
	if ok {
		return t
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.transports[backend]; ok {
		return t
	}
	t = newTransport()
	p.transports[backend] = t
	return t
}

// Client returns an http.Client backed by the per-backend transport with
// the given per-attempt timeout.
func (p *HTTPPool) Client(backend string, timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: p.Transport(backend),
		Timeout:   timeout,
	}
}

// CloseIdle closes idle connections on every transport. Called on shutdown.
func (p *HTTPPool) CloseIdle() {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, t := range p.transports {
		t.CloseIdleConnections()
	}
}

func newTransport() *http.Transport {
	d := &net.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           d.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		MaxConnsPerHost:       256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
}
