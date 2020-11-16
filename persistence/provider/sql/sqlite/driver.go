package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/verity/internal/x/sqlx"
	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/linger"
	"go.uber.org/multierr"
)

// Driver is an implementation of sql.Driver for SQLite.
var Driver = driver{
	lockUpdateInterval: 10 * time.Second,
	lockExpiryOffset:   5 * time.Second,
}

type driver struct {
	// lockUpdateInterval specifies how often an application lock's expiry timestamp
	// should be updated.
	lockUpdateInterval time.Duration

	// lockExpiryOffset specifies how far in the future locks should be set to
	// expire.
	lockExpiryOffset time.Duration
}

// Begin starts a transaction.
func (driver) Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, nil)
}

// LockApplication acquires an exclusive lock on an application's data.
//
// r is a function that releases the lock, if acquired successfully.
func (d driver) LockApplication(
	ctx context.Context,
	db *sql.DB,
	ak string,
) (r func() error, err error) {
	id, err := d.insertLock(ctx, db, ak)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan error)

	go func() {
		err := d.maintainLock(ctx, db, id)
		if err != nil {
			if ctx.Err() == nil {
				// If some error occurred before we expected to release the
				// lock, we forcefully close the database pool. It's likely
				// already "disconnected" or the data is corrupted.
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
func (d driver) insertLock(
	ctx context.Context,
	db *sql.DB,
	ak string,
) (_ int64, err error) {
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

	expires := now.Add(d.lockExpiryOffset)
	id, ok := sqlx.TryInsert(
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
	if !ok {
		return 0, persistence.ErrDataStoreLocked
	}

	sqlx.Commit(tx)

	return id, nil
}

// maintainLock periodically updates the expiry time of a lock record until ctx
// is canceled.
func (d driver) maintainLock(
	ctx context.Context,
	db *sql.DB,
	id int64,
) (err error) {
	defer sqlx.Recover(&err)

	var expires time.Time

	for {
		expires = time.Now().Add(d.lockExpiryOffset)

		err = d.updateLock(ctx, db, id, expires)
		if err != nil {
			break
		}

		err = linger.Sleep(ctx, d.lockUpdateInterval)
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
		d.deleteLock(deleteCtx, db, id),
	)
}

// updateLock updates the expiry time of a lock record.
func (d driver) updateLock(
	ctx context.Context,
	db *sql.DB,
	id int64,
	expires time.Time,
) (err error) {
	defer sqlx.Recover(&err)

	sqlx.ExecRow(
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
func (d driver) deleteLock(
	ctx context.Context,
	db *sql.DB,
	id int64,
) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM app_lock
		WHERE rowid = $1`,
		id,
	)
	return err
}

// CreateSchema creates the schema elements required by the SQLite driver.
func (driver) CreateSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE app_lock (
			app_key TEXT NOT NULL UNIQUE,
			expires INTEGER NOT NULL
		)`,
	)

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

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS app_lock`)

	dropAggregateSchema(ctx, db)
	dropEventSchema(ctx, db)
	dropOffsetSchema(ctx, db)
	dropProcessSchema(ctx, db)
	dropQueueSchema(ctx, db)

	return nil
}
