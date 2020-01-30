package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

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

	e := infix.New(app)

	if err := e.Run(ctx); err != context.Canceled {
		panic(err)
	}
}
