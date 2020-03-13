package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/configkit/api/discovery/simpledns"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/example"
	"github.com/dogmatiq/example/database"
	"github.com/dogmatiq/infix"
	"google.golang.org/grpc"
)

// newContext returns a cancelable context that is canceled when the process
// receives a SIGTERM or SIGINT.
func newContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case <-ctx.Done():
		case <-sig:
			cancel()
		}
	}()

	return ctx, cancel
}

func main() {
	ctx, cancel := newContext()
	defer cancel()

	app, err := example.NewApp(database.MustNew())
	if err != nil {
		panic(err)
	}

	e := infix.New(
		infix.WithApplication(app),
		infix.WithLogger(logging.DebugLogger),
		infix.WithDiscoverer(discover),
		infix.WithDialer(dial),
	)

	err = e.Run(ctx)

	if !errors.Is(err, context.Canceled) {
		panic(err)
	}
}

func discover(ctx context.Context, obs discovery.TargetObserver) error {
	d := &simpledns.Discoverer{
		QueryHost: "localhost",
		Observer:  obs,
	}

	return d.Run(ctx)
}

func dial(ctx context.Context, t *discovery.Target) (*grpc.ClientConn, error) {
	options := append(
		[]grpc.DialOption{
			grpc.WithInsecure(),
		},
		t.Options...,
	)

	return grpc.DialContext(ctx, t.Name, options...)
}
