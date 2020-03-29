package boltdb

import (
	"context"
	"sync"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
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

// EventStoreRepository returns the application's event store repository.
func (ds *dataStore) EventStoreRepository() eventstore.Repository {
	return &eventStoreRepository{ds.db, ds.appKey}
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

	r(string(ds.appKey))

	return nil
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
