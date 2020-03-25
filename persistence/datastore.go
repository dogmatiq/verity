package persistence

import (
	"context"
	"errors"
	"sync"

	"github.com/dogmatiq/infix/persistence/eventstore"
	"go.uber.org/multierr"
)

// ErrDataStoreClosed is returned when performing any persistence operation on a
// closed data-store.
var ErrDataStoreClosed = errors.New("data store is closed")

// DataStore is an interface used by the engine to persist and retrieve
// data for a specific application.
type DataStore interface {
	// EventStoreRepsotiory returns the application's event store repository.
	EventStoreRepository() eventstore.Repository

	// Begin starts a new transaction.
	Begin(ctx context.Context) (Transaction, error)

	// Close closes the data store.
	//
	// Closing a data-store prevents any writes to the data-store. Specifically,
	// DataStore.Begin() and Transaction.Commit() will return ErrDataStoreClosed
	// if the transaction's underlying data-store has been closed.
	//
	// The behavior of any other persistence operation on a closed data-store is
	// undefined.
	Close() error
}

// DataStoreSet is a collection of data-stores for several applications.
type DataStoreSet struct {
	Provider Provider

	m      sync.Mutex
	stores map[string]DataStore
}

// Get returns the data store for a given application.
//
// If the set already contains a data-store for the given application it is
// returned. Otherwise it is opened and added to the set. The caller is NOT
// reponsible for closing the data store.
func (s *DataStoreSet) Get(ctx context.Context, k string) (DataStore, error) {
	s.m.Lock()
	defer s.m.Unlock()

	if ds, ok := s.stores[k]; ok {
		return ds, nil
	}

	ds, err := s.Provider.Open(ctx, k)
	if err != nil {
		return nil, err
	}

	if s.stores == nil {
		s.stores = map[string]DataStore{}
	}

	s.stores[k] = ds

	return ds, nil
}

// Close closes all datastores in the set.
func (s *DataStoreSet) Close() error {
	s.m.Lock()
	defer s.m.Unlock()

	stores := s.stores
	s.stores = nil

	var err error
	for _, ds := range stores {
		err = multierr.Append(
			err,
			ds.Close(),
		)
	}

	return err
}
