package postgres

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
	"go.uber.org/multierr"
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
		if err != nil {
			err = multierr.Append(
				err,
				tx.Rollback(),
			)
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

// CreateSchema creates any SQL schema elements required by the driver.
func (driver) CreateSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	sqlx.Exec(ctx, db, `CREATE SCHEMA infix`)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE infix.app_lock_id (
			app_key TEXT NOT NULL PRIMARY KEY,
			lock_id SERIAL NOT NULL UNIQUE
		)`,
	)

	createAggregateSchema(ctx, db)
	createEventSchema(ctx, db)
	createOffsetSchema(ctx, db)
	createProcessSchema(ctx, db)
	createQueueSchema(ctx, db)

	return tx.Commit()
}

// DropSchema removes any SQL schema elements created by CreateSchema().
func (driver) DropSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS infix CASCADE`)
	return err
}
