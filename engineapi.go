package infix

import (
	"context"
	"fmt"
	"net"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/api"
	"github.com/dogmatiq/dodeca/logging"
	"google.golang.org/grpc"
)

// serveAPI runs the gRPC server.
func (e *Engine) serveAPI(ctx context.Context) error {
	s := grpc.NewServer(e.opts.ServerOptions...)

	// config []RichApplication to []Application
	configs := make([]configkit.Application, len(e.configs))
	for i, cfg := range e.configs {
		configs[i] = cfg
	}
	api.RegisterServer(s, configs...)

	lis, err := net.Listen("tcp", e.opts.ListenAddress)
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
		s.Stop()
	}()

	logging.Log(
		e.opts.Logger,
		"listening for API requests on %s",
		e.opts.ListenAddress,
	)

	err = s.Serve(lis)
	return fmt.Errorf("gRPC server stopped: %w", err)
}
