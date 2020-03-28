package postgres

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
)

// Driver is an implementation of sql.Driver for PostgreSQL.
var Driver errorConverter

type driver struct{}

// Begin starts a transaction.
func (driver) Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, nil)
}

// LockApplication acquires an exclusive lock on an application's data.
//
// r is a function that releases the lock, if acquired successfully.
func (driver) LockApplication(
	ctx context.Context,
	db *sql.DB,
	ak string,
) (r func() error, err error) {
	defer sqlx.Recover(&err)

	// Note: DO UPDATE is used because DO NOTHING excludes the row from the
	// RETURNING result set.
	id := sqlx.QueryInt64(
		ctx,
		db,
		`INSERT INTO infix.app_lock_id (
			app_key
		) VALUES (
			$1
		) ON CONFLICT (app_key) DO UPDATE SET
			app_key = excluded.app_key
		RETURNING lock_id`,
		ak,
	)

	tx := sqlx.Begin(ctx, db)
	defer func() {
		if r == nil {
			tx.Rollback()
		}
	}()

	if sqlx.QueryBool(
		context.Background(),
		tx,
		`SELECT pg_try_advisory_xact_lock($1)`,
		id,
	) {
		return tx.Rollback, nil
	}

	return nil, persistence.ErrDataStoreLocked
}
