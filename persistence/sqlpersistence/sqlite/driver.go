package sqlite

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/verity/internal/x/sqlx"
)

// Driver is an implementation of sql.Driver for SQLite.
var Driver = driver{}

type driver struct{}

// IsCompatibleWith returns nil if this driver can be used with db.
func (driver) IsCompatibleWith(ctx context.Context, db *sql.DB) error {
	// Verify that we're using SQLite and that $1-style placeholders are
	// supported.
	return db.QueryRowContext(
		ctx,
		`SELECT sqlite_version() WHERE 1 = $1`,
		1,
	).Err()
}

// Begin starts a transaction.
func (driver) Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, nil)
}

// CreateSchema creates the schema elements required by the SQLite driver.
func (driver) CreateSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback() // nolint:errcheck

	createAggregateSchema(ctx, db)
	createEventSchema(ctx, db)
	createOffsetSchema(ctx, db)
	createProcessSchema(ctx, db)
	createQueueSchema(ctx, db)

	return tx.Commit()
}

// DropSchema drops the schema elements required by the SQLite driver.
func (driver) DropSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	dropAggregateSchema(ctx, db)
	dropEventSchema(ctx, db)
	dropOffsetSchema(ctx, db)
	dropProcessSchema(ctx, db)
	dropQueueSchema(ctx, db)

	return nil
}
