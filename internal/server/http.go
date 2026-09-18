package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// HTTP wraps an HTTP handler in the shared server lifecycle.
type HTTP struct {
	name   string
	server *http.Server
}

func NewHTTP(name, addr string, handler http.Handler) *HTTP {
	return &HTTP{name: name, server: &http.Server{Addr: addr, Handler: h2c.NewHandler(handler, &http2.Server{}), ReadHeaderTimeout: 10 * time.Second}}
}

func (s *HTTP) Name() string { return s.name }
func (s *HTTP) Start(context.Context) error {
	err := s.server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func (s *HTTP) Shutdown(ctx context.Context) error { return s.server.Shutdown(ctx) }
