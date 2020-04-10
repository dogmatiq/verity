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
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix"
	"github.com/dogmatiq/infix/persistence/provider/memory"
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

type Deposit struct {
	AccountID string
	Amount    int
}

func (m Deposit) MessageDescription() string {
	return fmt.Sprintf("credit %s with %d", m.AccountID, m.Amount)
}

type AccountCredited struct {
	AccountID string
	Amount    int
}

func (m AccountCredited) MessageDescription() string {
	return fmt.Sprintf("acount %s credited %d", m.AccountID, m.Amount)
}

func run(ctx context.Context) error {
	app := &fixtures.Application{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("cashier", "507d5e32-0a6d-4eaa-bb22-ed1546928eea")

			c.RegisterIntegration(
				&fixtures.IntegrationMessageHandler{
					ConfigureFunc: func(c dogma.IntegrationConfigurer) {
						c.Identity("paypal", "f0e82d22-3687-4091-8341-2d6ccd720271")
						c.ConsumesCommandType(Deposit{})
						c.ProducesEventType(AccountCredited{})
					},
					HandleCommandFunc: func(
						ctx context.Context,
						sc dogma.IntegrationCommandScope,
						m dogma.Message,
					) error {
						sc.Log("handling a %T message", m)

						switch m := m.(type) {
						case Deposit:
							sc.RecordEvent(AccountCredited{
								AccountID: m.AccountID,
								Amount:    m.Amount,
							})
						default:
							panic(dogma.UnexpectedMessage)
						}

						return errors.New("OH NO")
					},
				},
			)
		},
	}

	e := infix.New(
		app,
		infix.WithPersistence(
			&memory.Provider{},
		),
		infix.WithLogger(logging.DebugLogger),
	)

	go func() {
		err := e.ExecuteCommand(
			ctx,
			Deposit{
				AccountID: "343-2434",
				Amount:    100,
			},
		)
		if err != nil {
			log.Fatal(err)
		}
	}()

	return e.Run(ctx)
}
