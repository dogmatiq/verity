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

	customerID   string
	customerName string

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

	rows, err := ui.DB.QueryContext(
		ui.ctx,
		`SELECT
			id,
			name
		FROM customer
		ORDER BY name`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []item

	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}

		k := fmt.Sprintf("%d", len(items)+1)
		v := fmt.Sprintf("sign in as '%s'", name)

		items = append(
			items,
			item{k, v, ui.loginAs(id, name)},
		)
	}

	rows.Close()

	items = append(items, item{"n", "open an account for a new customer", ui.openAccountForNewCustomer})
	items = append(items, item{"q", "quit", nil})

	return ui.askMenu(items...)
}

func (ui *UI) loginAs(id, name string) state {
	return func() (state, error) {
		ui.customerID = id
		ui.customerName = name
		return ui.customerMain, nil
	}
}

func (ui *UI) customerMain() (state, error) {
	ui.banner("OVERVIEW (%s)", ui.customerName)

	return ui.askMenu(
		item{"q", "sign out", ui.main},
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
