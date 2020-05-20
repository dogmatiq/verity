package sql

import (
	"context"
	"database/sql"
	"sync"

	"github.com/dogmatiq/infix/internal/x/sqlx"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// dataStore is an implementation of persistence.DataStore for SQL databases.
type dataStore struct {
	db     *sql.DB
	driver Driver
	appKey string

	m       sync.RWMutex
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
		release: r,
	}
}

// EventStoreRepository returns the application's event store repository.
func (ds *dataStore) EventStoreRepository() eventstore.Repository {
	return ds
}

// OffsetStoreRepository returns the application's event store repository.
func (ds *dataStore) OffsetStoreRepository() offsetstore.Repository {
	return ds
}

// QueueStoreRepository returns the application's queue store repository.
func (ds *dataStore) QueueStoreRepository() queuestore.Repository {
	return ds
}

// Persist commits a batch of operations atomically.
//
// If any one of the operations causes an optimistic concurrency conflict
// the entire batch is aborted and a ConflictError is returned.
func (ds *dataStore) Persist(
	ctx context.Context,
	b persistence.Batch,
) (_ persistence.Result, err error) {
	b.MustValidate()

	defer sqlx.Recover(&err)

	ds.m.RLock()
	defer ds.m.RUnlock()

	if ds.release == nil {
		return persistence.Result{}, persistence.ErrDataStoreClosed
	}

	tx, err := ds.driver.Begin(ctx, ds.db)
	if err != nil {
		return persistence.Result{}, err
	}
	defer tx.Rollback()

	c := &committer{
		tx:     tx,
		driver: ds.driver,
		appKey: ds.appKey,
	}

	if err := b.AcceptVisitor(ctx, c); err != nil {
		return persistence.Result{}, err
	}

	return c.result, tx.Commit()
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
	ds.m.Lock()
	defer ds.m.Unlock()

	if ds.release == nil {
		return persistence.ErrDataStoreClosed
	}

	r := ds.release
	ds.release = nil

	return r()
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
