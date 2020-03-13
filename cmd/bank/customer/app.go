package customer

import (
	"database/sql"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/example/domain"
	"github.com/dogmatiq/example/projections"
	pksql "github.com/dogmatiq/projectionkit/sql"
)

// App is an implementation of dogma.Application for the bank example.
type App struct {
	ProjectionDB *sql.DB
}

// Configure configures the Dogma engine for this application.
func (a *App) Configure(c dogma.ApplicationConfigurer) {
	p, err := pksql.New(
		a.ProjectionDB,
		&projections.CustomerProjectionHandler{},
		nil,
	)
	if err != nil {
		panic(err)
	}

	c.Identity("bank.customer", "db385bd2-59e6-400b-a573-cd9f5ac3381b")

	c.RegisterAggregate(domain.CustomerHandler{})

	c.RegisterProjection(p)
}
