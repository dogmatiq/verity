package persistence

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/marshalkit"
	"go.uber.org/multierr"
)

// Provider is an interface used by the engine to obtain application-specific
// DataStore instances.
type Provider interface {
	// Open returns a data-store for a specific application.
	Open(
		ctx context.Context,
		cfg configkit.RichApplication,
		m marshalkit.Marshaler,
	) (DataStore, error)
}

// DataStore is an interface used by the engine to persist and retrieve
// data for a specific application.
type DataStore interface {
	// EventStream returns the application's event stream.
	EventStream() eventstream.Stream

	// MessageQueue returns the application's queue of command and timeout
	// messages.
	MessageQueue() Queue

	// OffsetRepository returns the repository that stores the "progress" of
	// message handlers through the event streams they consume.
	OffsetRepository() OffsetRepository

	// Close closes the data store.
	Close() error
}

// DataStoreSet is a collection of data-stores for several applications.
type DataStoreSet struct {
	Provider  Provider
	Marshaler marshalkit.Marshaler

	m      sync.Mutex
	stores map[string]DataStore
}

// Get returns the data store for a given application.
func (s *DataStoreSet) Get(
	ctx context.Context,
	cfg configkit.RichApplication,
) (DataStore, error) {
	k := cfg.Identity().Key

	s.m.Lock()
	defer s.m.Unlock()

	if ds, ok := s.stores[k]; ok {
		return ds, nil
	}

	ds, err := s.Provider.Open(ctx, cfg, s.Marshaler)
	if err != nil {
		return nil, err
	}

	if s.stores == nil {
		s.stores = map[string]DataStore{}
	}

	s.stores[k] = ds

	return ds, nil
}

// Close closes all datastores in the XXX.
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
