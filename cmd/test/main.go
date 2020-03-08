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
		app,
		infix.WithLogger(logging.DebugLogger),
		infix.WithDiscoverer(
			func(ctx context.Context, obs discovery.TargetObserver) error {
				d := &simpledns.Discoverer{
					QueryHost: "localhost",
					Observer:  obs,
				}

				return d.Run(ctx)
			},
		),
	)

	err = e.Run(ctx)

	if !errors.Is(err, context.Canceled) {
		panic(err)
	}
}
