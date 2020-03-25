package sql

import (
	"database/sql"
	"fmt"

	"github.com/dogmatiq/infix/persistence/provider/sql/driver"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver/mysql"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver/postgres"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver/sqlite"
)

// Driver is used to interface with the underlying SQL database.
type Driver struct {
	StreamDriver driver.StreamDriver
}

// NewDriver returns the appropriate driver to use with the given database.
func NewDriver(db *sql.DB) (*Driver, error) {
	if mysql.IsCompatibleWith(db) {
		return &Driver{
			mysql.StreamDriver{},
		}, nil
	}

	if postgres.IsCompatibleWith(db) {
		return &Driver{
			postgres.StreamDriver{},
		}, nil
	}

	if sqlite.IsCompatibleWith(db) {
		return &Driver{
			sqlite.StreamDriver{},
		}, nil
	}

	return nil, fmt.Errorf(
		"can not deduce the appropriate SQL driver for %T",
		db.Driver(),
	)
}
