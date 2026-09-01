// Package cosmos_grpc is the Cosmos gRPC listener. It runs a transparent
// proxy via mwitkow/grpc-proxy, with a Director that consults stitch's
// selector + circuit-breaker stack.
//
// What this phase does:
//   - Accepts any registered or unknown service.
//   - Routes each RPC to a backend chosen by stitch (metadata or supported
//     protobuf-body heights, broadcast detection, circuit gating,
//     failover-by-redial).
//   - Reuses pooled *grpc.ClientConn per backend with idle eviction.
//
// What is deferred:
//   - gRPC-Web wrapping (small wrapper; phase 2b).
//   - Streaming-RPC failover after partial response (the subscription hub
//     in phase 5 covers this for ChainStream specifically).
package cosmos_grpc

import (
	"context"
	"errors"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/InjectiveLabs/stitch/internal/log"
)

// Server is the Cosmos gRPC listener.
type Server struct {
	dir *Director
	srv *grpc.Server
	lis net.Listener
}

// New constructs a server bound to addr. The listener is opened
// synchronously so callers can read Addr() immediately. Start blocks
// serving traffic.
func New(addr string, dir *Director) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cosmos_grpc listen %s: %w", addr, err)
	}
	srv := grpc.NewServer(
		grpc.UnknownServiceHandler(streamHandler(dir)),
		grpc.MaxRecvMsgSize(64*1024*1024),
		grpc.MaxSendMsgSize(64*1024*1024),
	)
	return &Server{dir: dir, srv: srv, lis: lis}, nil
}

// MustNew is the variant for callers (cmd start) that prefer a panic on
// bind failure during boot.
func MustNew(addr string, dir *Director) *Server {
	s, err := New(addr, dir)
	if err != nil {
		panic(err)
	}
	return s
}

func (s *Server) Name() string { return "cosmos_grpc" }

func (s *Server) Start(_ context.Context) error {
	log.L().Info("cosmos_grpc: listening", "addr", s.lis.Addr().String())
	if err := s.srv.Serve(s.lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	stopped := make(chan struct{})
	go func() {
		s.srv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		s.srv.Stop()
		return ctx.Err()
	}
}

// Addr returns the bound address. Stable from construction, since New
// opens the listener synchronously.
func (s *Server) Addr() string { return s.lis.Addr().String() }
