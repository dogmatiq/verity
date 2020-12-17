package mysql

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

	sqlx.Exec(
		ctx,
		db,
		`DELETE FROM app_lock
		WHERE expires_at <= CURRENT_TIMESTAMP(6)`,
	)

	id, ok := sqlx.TryInsert(
		ctx,
		db,
		`INSERT INTO app_lock SET
			app_key = ?,
			expires_at = CURRENT_TIMESTAMP(6) + INTERVAL ? SECOND
		ON DUPLICATE KEY UPDATE
			id = id`, // do nothing
		ak,
		ttl.Seconds(),
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

	// The "renewals" column forces MySQL to count the matching row as
	// "affected" even if the expires_at column isn't actually changed.
	return sqlx.TryExecRow(
		ctx,
		db,
		`UPDATE app_lock SET
			expires_at = CURRENT_TIMESTAMP(6) + INTERVAL ? SECOND,
			renewals = renewals + 1
		WHERE id = ?
		AND expires_at > CURRENT_TIMESTAMP(6)`,
		ttl.Seconds(),
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
		`DELETE FROM app_lock
		WHERE id = ?`,
		id,
	)
	return err
}

// createLockSchema creates schema elements for locks.
func createLockSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE app_lock (
			id         BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
			app_key    VARBINARY(255) NOT NULL UNIQUE,
			expires_at TIMESTAMP(6) NOT NULL,
			renewals   INTEGER NOT NULL DEFAULT 0
		) ENGINE=InnoDB`,
	)
}

// dropLockSchema drops  schema elements for locks.
func dropLockSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS app_lock`)
}
