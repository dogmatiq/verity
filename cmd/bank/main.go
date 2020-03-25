package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/configkit/api/discovery/static"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix"
	"github.com/dogmatiq/infix/cmd/bank/apps"
	"github.com/dogmatiq/infix/internal/testing/sqltest"
	"github.com/dogmatiq/infix/persistence/provider/memory"
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

	if err := run(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}

func run(ctx context.Context) error {
	if len(os.Args) != 2 {
		return errors.New("usage: bank [customer|account]")
	}
	appName := os.Args[1]

	db := sqltest.Open("postgres")
	defer db.Close()

	// TODO: return to using SQL
	// if err := postgres.CreateSchema(ctx, db); err != nil {
	// 	fmt.Println(err)
	// }

	app, err := newApp(db, appName)
	if err != nil {
		return err
	}

	// Build a list of gRPC server addresses for each application.
	addresses := map[string]string{
		"account":     "127.0.0.1:45001",
		"customer":    "127.0.0.1:45002",
		"transaction": "127.0.0.1:45003",
	}

	// Listen on the address for this application, then remove it to pass the
	// remaining addresses to the discoverer.
	listen := addresses[appName]
	delete(addresses, appName)
	discoverer := newDiscoverer(addresses)

	e := infix.New(
		app,
		infix.WithPersistence(
			&memory.Provider{},
			// TODO: return to using SQL.
			// &infixsql.Provider{
			// 	DB: db,
			// },
		),
		infix.WithNetworking(
			infix.WithListenAddress(listen),
			infix.WithDiscoverer(discoverer),
		),
		infix.WithLogger(logging.DebugLogger),
	)

	return e.Run(ctx)
}

// newApp constructs a new application by name.
func newApp(db *sql.DB, name string) (dogma.Application, error) {
	switch name {
	case "account":
		return &apps.AccountApp{ProjectionDB: db}, nil
	case "customer":
		return &apps.CustomerApp{ProjectionDB: db}, nil
	case "transaction":
		return &apps.TransactionApp{}, nil
	default:
		return nil, fmt.Errorf("unknown app: %s", name)
	}
}

// newDiscoverer returns a new discoverer which always "discovers" the other
// applications in the bank suite.
func newDiscoverer(addresses map[string]string) infix.Discoverer {
	return func(ctx context.Context, obs discovery.TargetObserver) error {
		d := &static.Discoverer{
			Observer: obs,
		}

		for _, addr := range addresses {
			d.Targets = append(
				d.Targets,
				&discovery.Target{
					Name: addr,
					Options: []grpc.DialOption{
						grpc.WithInsecure(),
					},
				},
			)
		}

		return d.Run(ctx)
	}
}
