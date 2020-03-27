package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/linger"
	"go.uber.org/multierr"
)

// Driver is an implementation of sql.Driver for SQLite.
var Driver driver

type driver struct{}

const (
	// lockUpdateInterval specifies how often an application lock's expiry timestamp
	// should be updated.
	lockUpdateInterval = 1 * time.Second

	// lockExpiryOffset specifies how far in the future locks should be set to
	// expire.
	lockExpiryOffset = 3 * time.Second
)

// LockApplication acquires an exclusive lock on an application's data.
//
// r is a function that releases the lock, if acquired successfully.
func (driver) LockApplication(
	ctx context.Context,
	db *sql.DB,
	ak string,
) (r func() error, err error) {
	id, err := insertLock(ctx, db, ak)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan error)

	go func() {
		err := maintainLock(ctx, db, id)
		if err != nil {
			if ctx.Err() == nil {
				// If some error occured before we expected to release the lock,
				// we forcefully close the database pool. It's likely already
				// "disconnected" or the data is corrupted.
				db.Close()
			} else if err == context.Canceled {
				// Otherwise, if the error was just the expected context
				// cancellation, don't report it as an error at all.
				err = nil
			}
		}

		done <- err
	}()

	return func() error {
		cancel()
		return <-done
	}, nil
}

// insertLock creates a lock record for an application.
func insertLock(
	ctx context.Context,
	db *sql.DB,
	ak string,
) (_ uint64, err error) {
	defer sqlx.Recover(&err)

	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	now := time.Now()
	sqlx.Exec(
		ctx,
		tx,
		`DELETE FROM app_lock
		WHERE expires <= $1`,
		now.Unix(),
	)

	expires := now.Add(lockExpiryOffset)
	res := sqlx.Exec(
		ctx,
		tx,
		`INSERT OR IGNORE INTO app_lock (
			app_key,
			expires
		) VALUES (
			$1, $2
		)`,
		ak,
		expires.Unix(),
	)

	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	if n == 0 {
		return 0, persistence.ErrDataStoreLocked
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, nil
	}

	sqlx.Commit(tx)

	return uint64(id), nil
}

// maintainLock periodically updates the expiry time of a lock record until ctx
// is canceled.
func maintainLock(
	ctx context.Context,
	db *sql.DB,
	id uint64,
) (err error) {
	defer sqlx.Recover(&err)

	var expires time.Time

	for {
		expires = time.Now().Add(lockExpiryOffset)

		err = updateLock(ctx, db, id, expires)
		if err != nil {
			break
		}

		err = linger.Sleep(ctx, lockUpdateInterval)
		if err != nil {
			break
		}
	}

	// Setup a new context for the deletion with a deadline equal to the last
	// expiry timestamp. Note that it is not derived from ctx.
	deleteCtx, cancelDelete := context.WithDeadline(context.Background(), expires)
	defer cancelDelete()

	return multierr.Append(
		err,
		deleteLock(deleteCtx, db, id),
	)
}

// updateLock updates the expiry time of a lock record.
func updateLock(
	ctx context.Context,
	db *sql.DB,
	id uint64,
	expires time.Time,
) (err error) {
	defer sqlx.Recover(&err)

	sqlx.UpdateRow(
		ctx,
		db,
		`UPDATE app_lock SET
			expires = $1
		WHERE rowid = $2`,
		expires.Unix(),
		id,
	)

	return nil
}

// deleteLock removes an application lock record.
func deleteLock(
	ctx context.Context,
	db *sql.DB,
	id uint64,
) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM app_lock
		WHERE rowid = $1`,
		id,
	)
	return err
}