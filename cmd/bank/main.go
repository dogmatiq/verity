package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dogmatiq/infix/persistence/provider/boltdb"

	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/configkit/api/discovery/static"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix"
	"github.com/dogmatiq/infix/cmd/bank/account"
	"github.com/dogmatiq/infix/cmd/bank/customer"
	"github.com/dogmatiq/infix/internal/sqltest"
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

	apps := os.Args[1:]
	networking := true
	if len(apps) == 0 {
		networking = false
		apps = append(apps, "customer", "account")
	}

	var (
		engineOptions = []infix.EngineOption{
			infix.WithPersistence(&boltdb.Provider{
				Path: "/tmp/infix.db",
			}),
			infix.WithLogger(logging.DebugLogger),
		}
		networkOptions = []infix.NetworkOption{
			infix.WithDialer(dial),
			infix.WithDiscoverer(discover),
		}
	)

	for _, app := range apps {
		switch app {
		case "account":
			engineOptions = append(
				engineOptions,
				infix.WithApplication(
					&account.App{
						ProjectionDB: db,
					},
				),
			)

			networkOptions = append(
				networkOptions,
				infix.WithListenAddress(accountListenAddress),
			)
		case "customer":
			engineOptions = append(
				engineOptions,
				infix.WithApplication(
					&customer.App{
						ProjectionDB: db,
					},
				),
			)

			networkOptions = append(
				networkOptions,
				infix.WithListenAddress(customerListenAddress),
			)
		default:
			fmt.Printf("unknown app: %s", os.Args[1])
			os.Exit(1)
		}
	}

	if networking {
		engineOptions = append(
			engineOptions,
			infix.WithNetworking(networkOptions...),
		)
	}

	e := infix.New(nil, engineOptions...)

	err := e.Run(ctx)

	if !errors.Is(err, context.Canceled) {
		fmt.Println(err)
		os.Exit(1)
	}
}

func discover(ctx context.Context, obs discovery.TargetObserver) error {
	d := &static.Discoverer{
		Observer: obs,
		Targets: []*discovery.Target{
			{Name: accountListenAddress},
			{Name: customerListenAddress},
		},
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
