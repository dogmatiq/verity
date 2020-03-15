package main

import (
	"context"
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
	"github.com/dogmatiq/infix/cmd/bank/account"
	"github.com/dogmatiq/infix/cmd/bank/customer"
	"github.com/dogmatiq/infix/internal/testing/sqltest"
	"github.com/dogmatiq/infix/persistence/provider/sql"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver/postgres"
	"google.golang.org/grpc"
)

const (
	accountListenAddress  = "127.0.0.1:45001"
	customerListenAddress = "127.0.0.1:45002"
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

	db := sqltest.Open("postgres")
	defer db.Close()

	if err := postgres.CreateSchema(ctx, db); err != nil {
		fmt.Println(err)
	}

	if len(os.Args) != 2 {
		fmt.Println("usage: bank [customer|account]")
		os.Exit(1)
	}

	var (
		listen   string
		discover string
		app      dogma.Application
	)

	switch os.Args[1] {
	case "account":
		listen = accountListenAddress
		discover = customerListenAddress
		app = &account.App{
			ProjectionDB: db,
		}
	case "customer":
		listen = customerListenAddress
		discover = accountListenAddress
		app = &customer.App{
			ProjectionDB: db,
		}
	default:
		fmt.Printf("unknown app: %s", os.Args[1])
		os.Exit(1)
	}

	discoverer := func(ctx context.Context, obs discovery.TargetObserver) error {
		d := &static.Discoverer{
			Observer: obs,
			Targets: []*discovery.Target{
				{
					Name: discover,
					Options: []grpc.DialOption{
						grpc.WithInsecure(),
					},
				},
			},
		}

		return d.Run(ctx)
	}

	e := infix.New(
		app,
		infix.WithPersistence(&sql.Provider{
			DB: db,
		}),
		infix.WithNetworking(
			infix.WithListenAddress(listen),
			infix.WithDiscoverer(discoverer),
		),
		infix.WithLogger(logging.DebugLogger),
	)

	err := e.Run(ctx)

	if !errors.Is(err, context.Canceled) {
		fmt.Println(err)
		os.Exit(1)
	}
}
