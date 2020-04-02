package ui

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/dogmatiq/example/messages/commands"
	"github.com/google/uuid"

	"github.com/dogmatiq/dogma"
)

type state func() (state, error)

// UI manages the states of the user interface.
type UI struct {
	DB       *sql.DB
	Executor dogma.CommandExecutor

	ctx context.Context
	m   sync.Mutex
	log []string
}

// Run starts the UI.
func (ui *UI) Run(ctx context.Context) error {
	ui.ctx = ctx

	defer func() {
		r := recover()
		if r != "quit" && r != nil {
			panic(r)
		}
	}()

	ui.println("")

	var err error
	st := ui.main

	for st != nil && err == nil {
		st, err = st()
	}

	return err
}

func (ui *UI) quit() {
	panic("quit")
}

func (ui *UI) execute(m dogma.Message) {
	if err := ui.Executor.ExecuteCommand(ui.ctx, m); err != nil {
		ui.appendLog(
			fmt.Sprintf(
				"could not enqueue command: %s",
				err,
			),
		)
	}
}

func (ui *UI) main() (state, error) {
	ui.banner("MAIN MENU")

	return ui.askMenu(
		item{"n", "open an account for a new customer", ui.openAccountForNewCustomer},
		item{"q", "quit", nil},
	)
}

func (ui *UI) openAccountForNewCustomer() (state, error) {
	ui.banner("OPEN ACCOUNT (NEW CUSTOMER)")

	cname, ok := ui.askText("Customer Name")
	if !ok {
		return ui.main, nil
	}

	aname := ui.askTextWithDefault("Account Name", "Savings")

	ui.execute(commands.OpenAccountForNewCustomer{
		CustomerID:   uuid.New().String(),
		CustomerName: cname,
		AccountID:    uuid.New().String(),
		AccountName:  aname,
	})

	return ui.main, nil
}
