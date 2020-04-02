// Package main executes a modified version of dogmatiq/example Banking
// application on an Infix engine.
package main

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/dogmatiq/example"
	"github.com/dogmatiq/example/database"
	"github.com/dogmatiq/infix"
	"github.com/dogmatiq/infix/cmd/bank/ui"
	infixsql "github.com/dogmatiq/infix/persistence/provider/sql"
	infixsqlite "github.com/dogmatiq/infix/persistence/provider/sql/sqlite"
	"github.com/dogmatiq/projectionkit/sql/sqlite"
	"golang.org/x/sync/errgroup"
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

	if err := run(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			os.Exit(1)
		}
	}
}

func run(ctx context.Context) error {
	db, err := sql.Open("sqlite3", "file:artifacts/bank.sqlite?mode=rwc")
	if err != nil {
		return err
	}
	defer db.Close()

	app, err := example.NewApp(db)
	if err != nil {
		return err
	}

	u := &ui.UI{
		DB: db,
	}

	// Create the schema for dogmatiq/infix.
	if err := infixsqlite.CreateSchema(ctx, db); err != nil {
		u.DebugString(err.Error())
	}

	// Create the schema for dogmatiq/projectionkit.
	if err := sqlite.CreateSchema(ctx, db); err != nil {
		u.DebugString(err.Error())
	}

	// Create the schema for dogmatiq/example.
	if err := database.CreateSchema(ctx, db); err != nil {
		u.DebugString(err.Error())
	}

	e := infix.New(
		app,
		infix.WithPersistence(
			&infixsql.Provider{
				DB: db,
			},
		),
		infix.WithLogger(u),
	)

	u.Executor = e

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := e.Run(ctx); err != nil {
			u.Log("infix has stopped: %s", err)
			return err
		}

		return nil
	})

	g.Go(func() error {
		defer cancel()
		return u.Run(ctx)
	})

	return g.Wait()
}
