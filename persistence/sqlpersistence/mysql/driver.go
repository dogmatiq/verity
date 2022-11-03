package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/verity/internal/x/sqlx"
)

// timeLayout is the Go date/time "layout" for MySQL date/time strings.
const timeLayout = "2006-01-02 15:04:05.999999"

// Driver is an implementation of sql.Driver for MySQL.
var Driver driver

type driver struct{}

// IsCompatibleWith returns nil if this driver can be used with db.
func (driver) IsCompatibleWith(ctx context.Context, db *sql.DB) error {
	// Verify that ?-style placeholders are supported.
	err := db.QueryRowContext(
		ctx,
		`SELECT ?`,
		1,
	).Err()

	if err != nil {
		return err
	}

	// Verify that we're using something compatible with MySQL (because the SHOW
	// VARIABLES syntax is supported) and that InnoDB is available.
	return db.QueryRowContext(
		ctx,
		`SHOW VARIABLES LIKE "innodb_page_size"`,
	).Err()
}

// Begin starts a transaction.
func (driver) Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, nil)
}

// CreateSchema creates any SQL schema elements required by the driver.
func (driver) CreateSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	createAggregateSchema(ctx, db)
	createEventSchema(ctx, db)
	createOffsetSchema(ctx, db)
	createProcessSchema(ctx, db)
	createQueueSchema(ctx, db)

	return nil
}

// DropSchema removes any SQL schema elements created by CreateSchema().
func (driver) DropSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	dropAggregateSchema(ctx, db)
	dropEventSchema(ctx, db)
	dropOffsetSchema(ctx, db)
	dropProcessSchema(ctx, db)
	dropQueueSchema(ctx, db)

	return nil
}
