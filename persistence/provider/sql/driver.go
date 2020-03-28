package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/infix/persistence/provider/sql/mysql"
	"github.com/dogmatiq/infix/persistence/provider/sql/postgres"
	"github.com/dogmatiq/infix/persistence/provider/sql/sqlite"
)

// Driver is used to interface with the underlying SQL database.
type Driver interface {
	eventStoreDriver

	// LockApplication acquires an exclusive lock on an application's data.
	//
	// r is a function that releases the lock, if acquired successfully.
	LockApplication(
		ctx context.Context,
		db *sql.DB,
		ak string,
	) (r func() error, err error)
}

// NewDriver returns the appropriate driver to use with the given database.
func NewDriver(db *sql.DB) (Driver, error) {
	if mysql.IsCompatibleWith(db) {
		return mysql.Driver, nil
	}

	if postgres.IsCompatibleWith(db) {
		return postgres.Driver, nil
	}

	if sqlite.IsCompatibleWith(db) {
		return sqlite.Driver, nil
	}

	return nil, fmt.Errorf(
		"can not deduce the appropriate SQL driver for %T",
		db.Driver(),
	)
}
