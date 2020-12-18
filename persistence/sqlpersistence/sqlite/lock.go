package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/verity/internal/x/sqlx"
)

// AcquireLock acquires an exclusive lock on an application's data.
//
// It returns the lock ID, which can be used in subsequent calls to RenewLock()
// and ReleaseLock().
//
// It returns false if the lock can not be acquired.
func (driver) AcquireLock(
	ctx context.Context,
	db *sql.DB,
	ak string,
	ttl time.Duration,
) (_ int64, _ bool, err error) {
	defer sqlx.Recover(&err)

	now := time.Now()

	sqlx.Exec(
		ctx,
		db,
		`DELETE FROM app_lock
		WHERE expires_at <= $1`,
		now.UnixNano(),
	)

	id, ok := sqlx.TryInsert(
		ctx,
		db,
		`INSERT INTO app_lock (
			app_key,
			expires_at
		) VALUES (
			$1, $2
		) ON CONFLICT (app_key) DO NOTHING`,
		ak,
		now.Add(ttl).UnixNano(),
	)

	return id, ok, nil

}

// RenewLock updates the expiry timestamp on a lock that has already been
// acquired.
//
// It returns false if the lock has not been acquired.
func (driver) RenewLock(
	ctx context.Context,
	db *sql.DB,
	id int64,
	ttl time.Duration,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	now := time.Now()

	return sqlx.TryExecRow(
		ctx,
		db,
		`UPDATE app_lock SET
			expires_at = $1
		WHERE id = $2
		AND expires_at > $3`,
		now.Add(ttl).UnixNano(),
		id,
		now.UnixNano(),
	), nil
}

// It returns false if the lock has not been acquired.
func (driver) ReleaseLock(
	ctx context.Context,
	db *sql.DB,
	id int64,
) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM app_lock
		WHERE id = $1`,
		id,
	)
	return err
}

// createLockSchema creates schema elements for locks.
func createLockSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE IF NOT EXISTS app_lock (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			app_key    TEXT NOT NULL UNIQUE,
			expires_at INTEGER NOT NULL
		)`,
	)
}

// dropLockSchema drops  schema elements for locks.
func dropLockSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS app_lock`)
}
