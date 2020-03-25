package memory

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/eventstore"
)

// dataStore is an implementation of persistence.DataStore for the in-memory
// persistence provider.
type dataStore struct {
	m    sync.RWMutex
	data *data
}

// EventStoreRepsotiory returns the application's event store repository.
func (ds *dataStore) EventStoreRepository() eventstore.Repository {
	return &eventStoreRepository{ds}
}

// Begin starts a new transaction.
func (ds *dataStore) Begin(ctx context.Context) (persistence.Transaction, error) {
	return &transaction{ds}, nil
}

// Close closes the data store.
//
// Closing a data-store prevents any writes to the data-store. Specifically,
// Transaction.Commit()  will return ErrDataStoreClosed if the transaction's
// underlying data-store has been closed.
//
// The behavior of any other persistence operation on a closed data-store is
// undefined.
func (ds *dataStore) Close() error {
	ds.m.Lock()
	defer ds.m.Unlock()

	if ds.data == nil {
		return persistence.ErrDataStoreClosed
	}

	ds.data.Unlock()
	ds.data = nil

	return nil
}

// get returns the data-store's internal data.
func (ds *dataStore) get() (*data, error) {
	ds.m.RLock()
	defer ds.m.RUnlock()

	if ds.data == nil {
		return nil, persistence.ErrDataStoreClosed
	}

	return ds.data, nil
}

// data encapsulates a single application's "persisted" data.
type data struct {
	locked uint32 // atomic
}

func (d *data) TryLock() bool {
	return atomic.CompareAndSwapUint32(&d.locked, 0, 1)
}

func (d *data) Unlock() {
	atomic.CompareAndSwapUint32(&d.locked, 1, 0)
}
