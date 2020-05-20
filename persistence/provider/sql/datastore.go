package sql

import (
	"context"
	"database/sql"
	"sync"

	"github.com/dogmatiq/infix/internal/refactor251"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
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

// AggregateStoreRepository returns application's aggregate store repository.
func (ds *dataStore) AggregateStoreRepository() aggregatestore.Repository {
	return &aggregateStoreRepository{ds.db, ds.driver, ds.appKey}
}

// EventStoreRepository returns the application's event store repository.
func (ds *dataStore) EventStoreRepository() eventstore.Repository {
	return &eventStoreRepository{ds.db, ds.driver, ds.appKey}
}

// OffsetStoreRepository returns the application's event store repository.
func (ds *dataStore) OffsetStoreRepository() offsetstore.Repository {
	return &offsetStoreRepository{ds.db, ds.driver, ds.appKey}
}

// QueueStoreRepository returns the application's queue store repository.
func (ds *dataStore) QueueStoreRepository() queuestore.Repository {
	return &queueStoreRepository{ds.db, ds.driver, ds.appKey}
}

// Persist commits a batch of operations atomically.
//
// If any one of the operations causes an optimistic concurrency conflict
// the entire batch is aborted and a ConflictError is returned.
func (ds *dataStore) Persist(
	ctx context.Context,
	batch persistence.Batch,
) (persistence.Result, error) {
	batch.MustValidate()

	if err := ds.checkOpen(); err != nil {
		return persistence.Result{}, err
	}

	tx := &transaction{ds: ds}
	defer tx.Rollback()

	if err := refactor251.PersistTx(ctx, tx, batch); err != nil {
		return persistence.Result{}, err
	}

	return tx.Commit(ctx)
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

// checkOpen returns an error if the data-store is closed.
func (ds *dataStore) checkOpen() error {
	ds.m.RLock()
	defer ds.m.RUnlock()

	if ds.release == nil {
		return persistence.ErrDataStoreClosed
	}

	return nil
}
