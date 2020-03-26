package sql

import (
	"database/sql"
	"fmt"

	"github.com/dogmatiq/infix/persistence/provider/sql/sqlite"
)

// Driver is used to interface with the underlying SQL database.
type Driver interface {
	eventStoreDriver
}

// NewDriver returns the appropriate driver to use with the given database.
func NewDriver(db *sql.DB) (Driver, error) {
	// if mysql.IsCompatibleWith(db) {
	// 	return &Driver{
	// 		mysql.StreamDriver{},
	// 	}, nil
	// }

	// if postgres.IsCompatibleWith(db) {
	// 	return &Driver{
	// 		postgres.StreamDriver{},
	// 	}, nil
	// }

	if sqlite.IsCompatibleWith(db) {
		return sqlite.Driver, nil
	}

	return nil, fmt.Errorf(
		"can not deduce the appropriate SQL driver for %T",
		db.Driver(),
	)
}
