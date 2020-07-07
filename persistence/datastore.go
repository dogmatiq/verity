package persistence

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/multierr"
)

// ErrDataStoreClosed is returned when performing any persistence operation on a
// closed data-store.
var ErrDataStoreClosed = errors.New("data store is closed")

// DataStore is an interface used by the engine to persist and retrieve data for
// a specific application.
type DataStore interface {
	AggregateRepository
	EventRepository
	OffsetRepository
	ProcessRepository
	QueueRepository

	Persister

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
// responsible for closing the data store.
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
