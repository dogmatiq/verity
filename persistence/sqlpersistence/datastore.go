package sqlpersistence

import (
	"context"
	"database/sql"
	"sync"

	"github.com/dogmatiq/verity/persistence"
)

// dataStore is an implementation of persistence.DataStore for SQL databases.
type dataStore struct {
	db     *sql.DB
	driver Driver
	appKey string

	closeM  sync.Mutex
	close   chan struct{}
	release func() error
}

// newDataStore returns a new data-store.
func newDataStore(
	db *sql.DB,
	d Driver,
	k string,
	r func() error,
) *dataStore {
	return &dataStore{
		db:      db,
		driver:  d,
		appKey:  k,
		close:   make(chan struct{}),
		release: r,
	}
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

	select {
	case <-ds.close:
		return persistence.ErrDataStoreClosed
	default:
	}

	close(ds.close)
	return ds.release()
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
	case <-ds.close:
		return persistence.ErrDataStoreClosed
	default:
	}

	// Create a new context that inherits from the provided context, and wire it
	// up to be canceled when the data store is closed.
	fnCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-ds.close:
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
		case <-ds.close:
			return persistence.ErrDataStoreClosed
		default:
		}
	}

	return err
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
