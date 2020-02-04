package api

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
)

// Run starts a gRPC server, stopping it when ctx is canceled.
func Run(ctx context.Context, addr string, s *grpc.Server) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("unable to start gRPC listener: %w", err)
	}
	defer lis.Close()

	// GOROUTINE EXIT STRATEGY: This goroutine always blocks on ctx being
	// canceled. A "clean shutdown" is triggered by this cancelation, and a
	// "dirty shutdown" occurs if s.Server() exists expectedly, in which case
	// ctx is also cancelled by the defer above.
	go func() {
		<-ctx.Done()
		s.Stop()
	}()

	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("gRPC server stopped: %w", err)
	}

	// A clean exit is ultimately due to ctx being canceled.
	return ctx.Err()
}
