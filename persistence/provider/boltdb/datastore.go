package boltdb

import (
	"context"
	"sync"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// dataStore is an implementation of persistence.DataStore for BoltDB.
type dataStore struct {
	db     *database
	appKey []byte

	m       sync.RWMutex
	release func(string) error
}

// newDataStore returns a new data-store.
func newDataStore(
	db *database,
	k string,
	r func(string) error,
) *dataStore {
	return &dataStore{
		db:      db,
		appKey:  []byte(k),
		release: r,
	}
}

// AggregateStoreRepository returns application's aggregate store repository.
func (ds *dataStore) AggregateStoreRepository() aggregatestore.Repository {
	return &aggregateStoreRepository{ds.db, ds.appKey}
}

// EventStoreRepository returns the application's event store repository.
func (ds *dataStore) EventStoreRepository() eventstore.Repository {
	return &eventStoreRepository{ds.db, ds.appKey}
}

// OffsetStoreRepository returns the application's event store repository.
func (ds *dataStore) OffsetStoreRepository() offsetstore.Repository {
	return &offsetStoreRepository{ds.db, ds.appKey}
}

// QueueStoreRepository returns the application's queue store repository.
func (ds *dataStore) QueueStoreRepository() queuestore.Repository {
	return &queueStoreRepository{ds.db, ds.appKey}
}

// Begin starts a new transaction.
func (ds *dataStore) Begin(ctx context.Context) (persistence.Transaction, error) {
	if err := ds.checkOpen(); err != nil {
		return nil, err
	}

	return &transaction{
		ds:     ds,
		appKey: ds.appKey,
	}, nil
}

// Close closes the data store.
//
// Closing a data-store immediately prevents new transactions from being
// started. Specifically, it causes Begin() to return ErrDataStoreClosed.
//
// The behavior of any other read or write operation on a closed data-store
// is implementation-defined.
//
// It is generally expected that all transactions have ended by the time the
// data-store is closed.
//
// Close() may block until any in-flight transactions are ended, or may
// prevent any such transactions from being committed.
func (ds *dataStore) Close() error {
	ds.m.Lock()
	defer ds.m.Unlock()

	if ds.release == nil {
		return persistence.ErrDataStoreClosed
	}

	r := ds.release
	ds.release = nil

	return r(string(ds.appKey))
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
