package memory

import (
	"context"
	"sync/atomic"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
)

// dataStore is an implementation of persistence.DataStore for the in-memory
// persistence provider.
type dataStore struct {
	db     *database
	closed uint32 // atomic
}

// newDataStore returns a new data-store.
func newDataStore(db *database) *dataStore {
	return &dataStore{
		db: db,
	}
}

// EventStoreRepository returns the application's event store repository.
func (ds *dataStore) EventStoreRepository() eventstore.Repository {
	return &eventStoreRepository{ds.db}
}

// QueueRepository returns the application's message queue repository.
func (ds *dataStore) QueueRepository() queue.Repository {
	return &queueRepository{ds.db}
}

// Begin starts a new transaction.
func (ds *dataStore) Begin(ctx context.Context) (persistence.Transaction, error) {
	if err := ds.checkOpen(); err != nil {
		return nil, err
	}

	return &transaction{ds: ds}, nil
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
	if !atomic.CompareAndSwapUint32(&ds.closed, 0, 1) {
		return persistence.ErrDataStoreClosed
	}

	ds.db.Close()

	return nil
}

// checkOpen returns an error if the data-store is closed.
func (ds *dataStore) checkOpen() error {
	if atomic.LoadUint32(&ds.closed) != 0 {
		return persistence.ErrDataStoreClosed
	}

	return nil
}
