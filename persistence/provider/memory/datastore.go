package memory

import (
	"context"
	"sync"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/eventstore"
)

// dataStore is an implementation of persistence.DataStore for the in-memory
// persistence provider.
type dataStore struct {
	m  sync.RWMutex
	db *database
}

// EventStoreRepsotiory returns the application's event store repository.
func (ds *dataStore) EventStoreRepository() eventstore.Repository {
	return &eventStoreRepository{ds}
}

// Begin starts a new transaction.
func (ds *dataStore) Begin(ctx context.Context) (persistence.Transaction, error) {
	return &transaction{ds: ds}, nil
}

// Close closes the data store.
//
// Closing a data-store prevents any writes to the data-store. Specifically,
// Transaction.Commit() will return ErrDataStoreClosed if the transaction's
// underlying data-store has been closed.
//
// The behavior of any other persistence operation on a closed data-store is
// undefined.
func (ds *dataStore) Close() error {
	ds.m.Lock()
	defer ds.m.Unlock()

	if ds.db == nil {
		return persistence.ErrDataStoreClosed
	}

	ds.db.Close()
	ds.db = nil

	return nil
}

// db returns the application's database.
//
// It returns an error if the data-store is closed.
func (ds *dataStore) database() (*database, error) {
	ds.m.RLock()
	defer ds.m.RUnlock()

	if ds.db == nil {
		return nil, persistence.ErrDataStoreClosed
	}

	return ds.db, nil
}
