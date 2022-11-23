package postgres

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/verity/internal/x/sqlx"
)

// Driver is an implementation of sql.Driver for PostgreSQL.
var Driver errorConverter

type driver struct{}

// IsCompatibleWith returns nil if this driver can be used with db.
func (driver) IsCompatibleWith(ctx context.Context, db *sql.DB) error {
	// Verify that we're using PostgreSQL and that $1-style placeholders are
	// supported.
	return db.QueryRowContext(
		ctx,
		`SELECT pg_backend_pid() WHERE 1 = $1`,
		1,
	).Err()
}

// Begin starts a transaction.
func (driver) Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, nil)
}

// CreateSchema creates any SQL schema elements required by the driver.
func (driver) CreateSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback() // nolint:errcheck

	sqlx.Exec(ctx, db, `CREATE SCHEMA IF NOT EXISTS verity`)

	createAggregateSchema(ctx, db)
	createEventSchema(ctx, db)
	createOffsetSchema(ctx, db)
	createProcessSchema(ctx, db)
	createQueueSchema(ctx, db)

	return tx.Commit()
}

// DropSchema removes any SQL schema elements created by CreateSchema().
func (driver) DropSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS verity CASCADE`)
	return err
}
