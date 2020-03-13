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
func serveAPI(ctx context.Context, opts *engineOptions) error {
	s := grpc.NewServer(opts.Network.ServerOptions...)

	// config []RichApplication to []Application
	configs := make([]configkit.Application, len(opts.AppConfigs))
	for i, cfg := range opts.AppConfigs {
		configs[i] = cfg
	}
	api.RegisterServer(s, configs...)

	lis, err := net.Listen("tcp", opts.Network.ListenAddress)
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
		opts.Logger,
		"listening for API requests on %s",
		opts.Network.ListenAddress,
	)

	err = s.Serve(lis)

	// If the server exists cleanly, it is because Stop() is called, which only
	// happens when the context is canceled.
	if err == nil {
		<-ctx.Done()
		err = ctx.Err()
	}

	return fmt.Errorf("gRPC server stopped: %w", err)
}
