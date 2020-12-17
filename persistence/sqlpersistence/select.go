package sqlpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/verity/persistence/sqlpersistence/mysql"
	"github.com/dogmatiq/verity/persistence/sqlpersistence/postgres"
	"github.com/dogmatiq/verity/persistence/sqlpersistence/sqlite"
	"go.uber.org/multierr"
)

// builtInDrivers is a list of the built-in drivers.
var builtInDrivers = []Driver{
	mysql.Driver,
	postgres.Driver,
	sqlite.Driver,
}

// selectDriver returns the appropriate driver implementation to use with the
// given database from a list of candidate drivers.
func selectDriver(ctx context.Context, db *sql.DB) (Driver, error) {
	var err error

	// First, ping the database to see if it's even usable. We'll never be able
	// to detect what the server is if we can't use it.
	if e := db.PingContext(ctx); e != nil {
		err = multierr.Append(err, fmt.Errorf("could not ping the server: %w", e))
	}

	if err == nil {
		for _, d := range builtInDrivers {
			e := d.IsCompatibleWith(ctx, db)
			if e == nil {
				return d, nil
			}

			err = multierr.Append(err, fmt.Errorf(
				"incompatible with %T: %w",
				d,
				e,
			))
		}
	}

	return nil, multierr.Append(err, fmt.Errorf(
		"could not find a driver that is compatible with %T",
		db.Driver(),
	))
}
