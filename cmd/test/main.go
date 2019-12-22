package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/dogmatiq/example"
	"github.com/dogmatiq/example/database"
	"github.com/dogmatiq/infix"
)

func main() {
	app, err := example.NewApp(database.MustNew())
	if err != nil {
		panic(app)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	go func() {
		select {
		case <-ctx.Done():
		case <-sig:
			cancel()
		}
	}()

	e := &infix.Engine{
		Application: app,
	}

	if err := e.Run(ctx); err != context.Canceled {
		panic(err)
	}
}
