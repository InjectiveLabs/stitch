// Package server holds the protocol-server lifecycle manager. Each phase
// adds concrete servers (RPC, REST, gRPC, etc.) by satisfying the Server
// interface; the manager handles signal-driven startup/shutdown for all of
// them.
package server

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/decentrio/stitch/internal/log"
)

// Server is one long-running protocol listener. Start blocks until the
// listener exits or ctx is cancelled. Shutdown is called once per server
// when the manager begins draining; it must be idempotent and bounded.
type Server interface {
	Name() string
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// Manager orchestrates a set of Servers and translates SIGINT/SIGTERM into a
// drain → shutdown sequence. SIGHUP triggers an optional reload callback.
type Manager struct {
	servers        []Server
	reload         func() error
	shutdownGrace  time.Duration
}

func New(shutdownGrace time.Duration) *Manager {
	return &Manager{shutdownGrace: shutdownGrace}
}

func (m *Manager) Add(s Server) { m.servers = append(m.servers, s) }

// OnReload installs a callback fired on SIGHUP. nil means SIGHUP is ignored.
func (m *Manager) OnReload(f func() error) { m.reload = f }

// Run blocks until every server exits or a termination signal is received.
// It returns the first non-nil server error or nil on clean shutdown.
func (m *Manager) Run(parent context.Context) error {
	if len(m.servers) == 0 {
		return errors.New("manager: no servers registered")
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(sigCh)

	g, gctx := errgroup.WithContext(ctx)

	for i := range m.servers {
		s := m.servers[i]
		g.Go(func() error {
			log.L().Info("starting server", "server", s.Name())
			if err := s.Start(gctx); err != nil {
				log.L().Error("server stopped with error", "server", s.Name(), "err", err.Error())
				return err
			}
			log.L().Info("server stopped", "server", s.Name())
			return nil
		})
	}

	g.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return nil
			case sig := <-sigCh:
				switch sig {
				case syscall.SIGHUP:
					if m.reload == nil {
						log.L().Info("SIGHUP ignored: no reload handler")
						continue
					}
					log.L().Info("SIGHUP: reloading config")
					if err := m.reload(); err != nil {
						log.L().Error("reload failed", "err", err.Error())
					} else {
						log.L().Info("reload complete")
					}
				default:
					log.L().Info("shutdown signal received", "signal", sig.String())
					m.drain()
					return nil
				}
			}
		}
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (m *Manager) drain() {
	ctx, cancel := context.WithTimeout(context.Background(), m.shutdownGrace)
	defer cancel()
	for _, s := range m.servers {
		s := s
		go func() {
			if err := s.Shutdown(ctx); err != nil {
				log.L().Error("shutdown error", "server", s.Name(), "err", err.Error())
			}
		}()
	}
	<-ctx.Done()
}
