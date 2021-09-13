package postgres

import (
	"context"
	"database/sql"
	"fmt"
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

	sqlx.Exec(
		ctx,
		db,
		`DELETE FROM verity.app_lock
		WHERE expires_at <= CURRENT_TIMESTAMP`,
	)

	row := db.QueryRowContext(
		ctx,
		`INSERT INTO verity.app_lock (
			app_key,
			expires_at
		) VALUES (
			$1, CURRENT_TIMESTAMP + $2
		) ON CONFLICT (app_key) DO NOTHING
		RETURNING id`,
		ak,
		fmt.Sprintf(
			"%d MICROSECONDS",
			ttl.Microseconds(),
		),
	)

	var id int64
	err = row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}

	return id, true, err
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

	return sqlx.TryExecRow(
		ctx,
		db,
		`UPDATE verity.app_lock SET
			expires_at = CURRENT_TIMESTAMP + $1
		WHERE id = $2
		AND expires_at > CURRENT_TIMESTAMP`,
		fmt.Sprintf(
			"%d MICROSECONDS",
			ttl.Microseconds(),
		),
		id,
	), nil
}

// ReleaseLock releases a lock that was previously acquired.
func (driver) ReleaseLock(
	ctx context.Context,
	db *sql.DB,
	id int64,
) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM verity.app_lock
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
		`CREATE TABLE IF NOT EXISTS verity.app_lock (
			id         BIGSERIAL PRIMARY KEY,
			app_key    TEXT NOT NULL UNIQUE,
			expires_at TIMESTAMP NOT NULL
		)`,
	)
}
