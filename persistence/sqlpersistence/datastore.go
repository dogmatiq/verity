package sqlpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/verity/persistence"
	"go.uber.org/multierr"
)

// dataStore is an implementation of persistence.DataStore for SQL databases.
type dataStore struct {
	db      *sql.DB
	driver  Driver
	appKey  string
	lockID  int64
	lockTTL time.Duration

	closeM     sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	closeCause error
	release    func() error
}

// newDataStore returns a new data-store.
func newDataStore(
	db *sql.DB,
	d Driver,
	k string,
	lockID int64,
	lockTTL time.Duration,
	r func() error,
) *dataStore {
	ctx, cancel := context.WithCancel(context.Background())

	ds := &dataStore{
		db:      db,
		driver:  d,
		appKey:  k,
		lockID:  lockID,
		lockTTL: lockTTL,
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
		release: r,
	}

	go ds.maintainLock()

	return ds
}

// Persist commits a batch of operations atomically.
//
// If any one of the operations causes an optimistic concurrency conflict
// the entire batch is aborted and a ConflictError is returned.
func (ds *dataStore) Persist(
	ctx context.Context,
	b persistence.Batch,
) (persistence.Result, error) {
	b.MustValidate()

	c := &committer{
		driver: ds.driver,
		appKey: ds.appKey,
	}

	return c.result, ds.withDB(
		ctx,
		func(ctx context.Context, db *sql.DB) error {
			tx, err := ds.driver.Begin(ctx, db)
			if err != nil {
				return err
			}
			defer tx.Rollback() // nolint:errcheck

			c.tx = tx
			if err := b.AcceptVisitor(ctx, c); err != nil {
				return err
			}

			return tx.Commit()
		},
	)
}

// Close closes the data store.
//
// Closing a data-store causes any future calls to Persist() to return
// ErrDataStoreClosed.
//
// The behavior read operations on a closed data-store is
// implementation-defined.
//
// In general use it is expected that all pending calls to Persist() will
// have finished before a data-store is closed. Close() may block until any
// in-flight calls to Persist() return, or may prevent any such calls from
// succeeding.
func (ds *dataStore) Close() error {
	ds.closeM.Lock()
	defer ds.closeM.Unlock()

	if ds.closeCause != nil {
		return persistence.ErrDataStoreClosed
	}

	ds.cancel()
	<-ds.done

	// Forcefully release the current lock, allowing up to the lockTTL period to
	// do so. Any longer and the lock will expire anyway.
	ctx, cancel := context.WithTimeout(context.Background(), ds.lockTTL)
	defer cancel()

	// Release the lock *before* ds.release() is called, as it may close the DB.
	err := ds.driver.ReleaseLock(ctx, ds.db, ds.lockID)

	return multierr.Append(
		err,
		ds.release(),
	)
}

// withDB calls fn with the database that should be used by this data-store.
//
// It returns an error if the data-store is already closed. The context passed
// to fn is canceled if the data-store is closed during execution.
func (ds *dataStore) withDB(
	ctx context.Context,
	fn func(ctx context.Context, db *sql.DB) error,
) error {
	// First, we check if the data-store has already been closed by checking if
	// it's context is canceled.
	select {
	case <-ds.done:
		return ds.closeCause
	default:
	}

	// Create a new context that inherits from the provided context, and wire it
	// up to be canceled when the data store is closed.
	fnCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-ds.done:
			// Cancel this new context if the data-store is closed.
			cancel()
		case <-fnCtx.Done():
			// Do nothing if fnCtx is canceled before the data-store is closed,
			// either because this function returns or its parent is canceled.
		}
	}()

	// Call the user-supplied function.
	err := fn(fnCtx, ds.db)

	// If the error was a context cancelation but the provided context was NOT
	// canceled AND the data-store was closed, then we assume that it was the
	// data-store closing that triggered the error and report the cause of the
	// closure.
	if err == context.Canceled && ctx.Err() != context.Canceled {
		select {
		case <-ds.done:
			return ds.closeCause
		default:
		}
	}

	return err
}

// maintainLock periodically renews the data-store's lock on the application
// data. If the lock can not be renewed the data-store is closed.
func (ds *dataStore) maintainLock() {
	defer close(ds.done)
	defer ds.cancel()

	for {
		ok, err := ds.driver.RenewLock(
			ds.ctx,
			ds.db,
			ds.lockID,
			ds.lockTTL,
		)

		if err == context.Canceled {
			ds.closeCause = persistence.ErrDataStoreClosed
			return
		}

		if err != nil {
			ds.closeCause = fmt.Errorf("unable to renew data-store lock: %w", err)
			return
		}

		if !ok {
			ds.closeCause = errors.New("unable to renew expired data-store lock")
			return
		}

		if err := linger.Sleep(ds.ctx, ds.lockTTL/2); err != nil {
			ds.closeCause = persistence.ErrDataStoreClosed
			return
		}
	}
}

// committer is an implementation of persitence.OperationVisitor that
// applies operations to the database.
//
// It is expected that the operations have already been validated using
// validator.
type committer struct {
	tx     *sql.Tx
	driver Driver
	appKey string
	result persistence.Result
}
