// Package main executes a modified version of dogmatiq/example Banking
// application on an Infix engine.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/example"
	"github.com/dogmatiq/example/cmd/bank/ui"
	"github.com/dogmatiq/example/database"
	"github.com/dogmatiq/infix"
	"github.com/dogmatiq/infix/internal/testing/sqltest"
	infixsql "github.com/dogmatiq/infix/persistence/provider/sql"
	infixsqlite "github.com/dogmatiq/infix/persistence/provider/sql/sqlite"
	"github.com/dogmatiq/projectionkit/sql/sqlite"
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
			fmt.Println(err)
			os.Exit(1)
		}
	}
}

func run(ctx context.Context) error {
	db, _, close := sqltest.Open("sqlite3")
	defer close()

	// Create the schema for dogmatiq/infix.
	if err := infixsqlite.CreateSchema(ctx, db); err != nil {
		fmt.Println(err)
	}

	// Create the schema for dogmatiq/projectionkit.
	if err := sqlite.CreateSchema(ctx, db); err != nil {
		fmt.Println(err)
	}

	// Create the schema for dogmatiq/example.
	if err := database.CreateSchema(ctx, db); err != nil {
		fmt.Println(err)
	}

	app, err := example.NewApp(db)
	if err != nil {
		return err
	}

	go ui.Run(db, nil)

	e := infix.New(
		app,
		infix.WithPersistence(
			&infixsql.Provider{
				DB: db,
			},
		),
		infix.WithLogger(
			&logging.StandardLogger{
				Target: log.New(os.Stderr, "", 0),
			},
		),
	)

	return e.Run(ctx)
}
