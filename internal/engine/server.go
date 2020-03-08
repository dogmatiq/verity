package engine

import (
	"context"
	"fmt"
	"net"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/api"
	"github.com/dogmatiq/dodeca/logging"
	"google.golang.org/grpc"
)

// Server encapsulates bootstrapping code for the engine's gRPC server.
type Server struct {
	ListenAddress string
	Options       []grpc.ServerOption
	Configs       []configkit.Application
	Logger        logging.Logger

	server *grpc.Server
}

// Run hosts the gRPC server until ctx is canceled.
func (s *Server) Run(ctx context.Context) error {
	logging.Log(
		s.Logger,
		"listening for API requests on %s",
		s.ListenAddress,
	)

	s.server = grpc.NewServer()

	api.RegisterServer(s.server, s.Configs...)
	// TODO: Register the messaging services.

	return s.serve(ctx)
}

// serve runs a gRPC server until ctx is canceled.
func (s *Server) serve(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.ListenAddress)
	if err != nil {
		return fmt.Errorf("unable to start gRPC listener: %w", err)
	}
	defer lis.Close()

	// Create a context that is guaranteed to be cancelled when this function
	// exits. This prevents a leak in the goroutine below when the server exits
	// prematurely.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Stop the server when the ctx is canceled from the outside.
	go func() {
		<-ctx.Done()
		s.server.Stop()
	}()

	err = s.server.Serve(lis)
	return fmt.Errorf("gRPC server stopped: %w", err)
}
