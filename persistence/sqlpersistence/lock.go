package sqlpersistence

import (
	"context"
	"database/sql"
	"time"
)

// LockDriver is the subset of the Driver interface that is concerned with
// application locking.
type LockDriver interface {
	// AcquireLock acquires an exclusive lock on an application's data.
	//
	// It returns the lock ID, which can be used in subsequent calls to
	// RenewLock() and ReleaseLock().
	//
	// It returns false if the lock can not be acquired.
	AcquireLock(
		ctx context.Context,
		db *sql.DB,
		ak string,
		ttl time.Duration,
	) (int64, bool, error)

	// RenewLock updates the expiry timestamp on a lock that has already been
	// acquired.
	//
	// It returns false if the lock has not been acquired.
	RenewLock(
		ctx context.Context,
		db *sql.DB,
		id int64,
		ttl time.Duration,
	) (bool, error)

	// ReleaseLock releases a lock that was previously acquired.
	ReleaseLock(
		ctx context.Context,
		db *sql.DB,
		id int64,
	) error
}
