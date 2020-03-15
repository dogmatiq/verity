package apps

import (
	"database/sql"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/example/domain"
	"github.com/dogmatiq/example/projections"
	pksql "github.com/dogmatiq/projectionkit/sql"
)

// AccountApp is an implementation of dogma.Application for the bank example.
type AccountApp struct {
	ProjectionDB *sql.DB
}

// Configure configures the Dogma engine for this application.
func (a *AccountApp) Configure(c dogma.ApplicationConfigurer) {
	p, err := pksql.New(
		a.ProjectionDB,
		&projections.AccountProjectionHandler{},
		nil,
	)
	if err != nil {
		panic(err)
	}

	c.Identity("bank.account", "6541a208-d4c2-46c4-a31e-372230efcd68")

	c.RegisterAggregate(domain.AccountHandler{})
	c.RegisterProcess(domain.OpenAccountForNewCustomerProcessHandler{})
	c.RegisterProjection(p)
}
