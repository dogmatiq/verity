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
		// If we can ping the server we move on to checking each driver for
		// compatibility in its own goroutine.
		//
		// We setup a context that we can use to bail early if we find a
		// compatible driver.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// And a channel of results from each driver.
		type result struct {
			driver Driver
			err    error
		}
		results := make(chan result, len(builtInDrivers))

		// We start a goroutine in which we call each driver's
		// IsCompatibleWith() method.
		for _, d := range builtInDrivers {
			d := d // capture loop variable

			go func() {
				results <- result{
					d,
					d.IsCompatibleWith(ctx, db),
				}
			}()
		}

		// We need to keep track of how many more results we expect to see so we
		// know when it's time to stop waiting.
		pending := len(builtInDrivers)

		for {
			select {
			case <-ctx.Done():
				// We've run out of time, bail now. Any pending goroutines will
				// be stopped via cancelation of ctx on defer.
				return nil, ctx.Err()

			case r := <-results:
				// We've received a result from one of the goroutines. Decrement
				// the pending counter.
				pending--

				// This driver is compatible with db, bail now. Again, the
				// pending goroutines will be stopped via cancelation of ctx on
				// defer.
				if r.err == nil {
					return r.driver, nil
				}

				// Otherwise, add this driver's error to the list of drivers
				// we'll return if we don't find any compatible driver.
				err = multierr.Append(err, fmt.Errorf(
					"incompatible with %T: %w",
					r.driver,
					r.err,
				))

				// If pending is 0 there's nothing left to wait for.
				if pending == 0 {
					break
				}
			}
		}
	}

	return nil, multierr.Append(err, fmt.Errorf(
		"could not find a driver that is compatible with %T",
		db.Driver(),
	))
}
