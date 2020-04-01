package ui

import (
	"context"
	"database/sql"
	"sync"

	"github.com/dogmatiq/example/messages/commands"
	"github.com/google/uuid"

	"github.com/dogmatiq/dogma"
)

type state func(ctx context.Context) (state, error)

// UI manages the states of the user interface.
type UI struct {
	DB       *sql.DB
	Executor dogma.CommandExecutor

	m   sync.Mutex
	log []string
}

// Run starts the UI.
func (ui *UI) Run(ctx context.Context) error {
	ui.println("")

	var err error
	st := ui.main

	for st != nil && err == nil {
		st, err = st(ctx)
	}

	return err
}

func (ui *UI) execute(ctx context.Context, m dogma.Message) {
	if err := ui.Executor.ExecuteCommand(ctx, m); err != nil {
		ui.appendLog(err.Error())
	}
}

func (ui *UI) main(ctx context.Context) (state, error) {
	ui.banner("MAIN MENU")

	return ui.askMenu(
		item{"n", "open an account for a new customer", ui.openAccountForNewCustomer},
	)
}

func (ui *UI) openAccountForNewCustomer(ctx context.Context) (state, error) {
	ui.banner("OPEN ACCOUNT (NEW CUSTOMER)")

	cname, ok := ui.askText("Customer Name")
	if !ok {
		return ui.main, nil
	}

	aname := ui.askTextWithDefault("Account Name", "Savings")

	ui.execute(ctx, commands.OpenAccountForNewCustomer{
		CustomerID:   uuid.New().String(),
		CustomerName: cname,
		AccountID:    uuid.New().String(),
		AccountName:  aname,
	})

	return ui.main, nil
}
