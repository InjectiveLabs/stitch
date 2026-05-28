// Package chainstream is the injective.stream.v* gRPC listener with
// upstream-failure resume.
//
// What this phase does (5b):
//
//   - Accepts any service/method matching /injective.stream.v*.Stream*.
//   - Reads the client's StreamRequest as opaque bytes (raw codec).
//   - Loops over selector candidates, dialing each and replaying the
//     StreamRequest after every upstream failure.
//   - For each StreamResponse, extracts block_height (proto field 1) via
//     protowire and drops responses with height ≤ cursor — the client
//     sees a continuous monotonic stream.
//
// What is intentionally absent:
//
//   - Multicast: every client gets its own upstream connection. A real
//     phase-5d implementation would canonicalize StreamRequest fields
//     and coalesce identical filters onto a shared upstream.
//   - Synthetic gap events: if the new upstream's earliest available
//     block is past the cursor, today the relay is silent. Adding a
//     stitch-minted "subscription_gap" envelope is a phase-5d task once
//     we agree on a wire shape.
//   - Per-method routing for non-stream RPCs that happen to share the
//     same listener — those are forwarded to the first eligible backend.
package chainstream

import (
	"context"
	"errors"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/pool"
	"github.com/decentrio/stitch/internal/selector"
)

// ChainStreamServiceNames lists the gRPC service prefixes we serve. Used
// by tests and `stitch methods` introspection.
var ChainStreamServiceNames = []string{
	"injective.stream.v1beta1.Stream",
	"injective.stream.v2.Stream",
}

// Server is the ChainStream gRPC listener.
type Server struct {
	dir *Director
	srv *grpc.Server
	lis net.Listener
}

// New constructs a chainstream server bound to addr. The listener is
// opened synchronously (eager bind) so callers can read Addr() right
// after construction.
func New(addr string, sel selector.Selector, cm *circuit.Manager, p *pool.GRPCPool) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("chainstream listen %s: %w", addr, err)
	}
	dir := NewDirector(sel, cm, p)
	srv := grpc.NewServer(
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(dir.Handler()),
		grpc.MaxRecvMsgSize(64*1024*1024),
		grpc.MaxSendMsgSize(64*1024*1024),
	)
	return &Server{dir: dir, srv: srv, lis: lis}, nil
}

func (s *Server) Name() string { return "chainstream" }

func (s *Server) Start(_ context.Context) error {
	log.L().Info("chainstream: listening", "addr", s.lis.Addr().String())
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

// Addr returns the bound address — stable from construction.
func (s *Server) Addr() string { return s.lis.Addr().String() }

// Director exposes the per-call orchestrator (selector + pool) for
// composition in cmd/start.
func (s *Server) Director() *Director { return s.dir }

// metadataDebug returns a copy of incoming metadata for log lines.
func metadataDebug(ctx context.Context) metadata.MD {
	md, _ := metadata.FromIncomingContext(ctx)
	return md.Copy()
}
