package grpcx

import (
	"context"
	"net"

	"google.golang.org/grpc"
)

// Serve runs s until ctx is canceled or an error occurs.
// The caller must never call s.Stop() or s.GracefulStop().
func Serve(
	ctx context.Context,
	lis net.Listener,
	s *grpc.Server,
) error {
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

	err := s.Serve(lis)

	// If the server exists cleanly, it is because Stop() is called, which only
	// happens when the context is canceled.
	if err == nil {
		<-ctx.Done()
		err = ctx.Err()
	}

	return err
}
