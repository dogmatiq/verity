package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// ProviderStub is a test implementation of the persistence.Provider interface.
type ProviderStub struct {
	persistence.Provider

	OpenFunc func(context.Context, string) (persistence.DataStore, error)
}

// Open returns a data-store for a specific application.
func (p *ProviderStub) Open(ctx context.Context, k string) (persistence.DataStore, error) {
	if p.OpenFunc != nil {
		return p.OpenFunc(ctx, k)
	}

	if p.Provider != nil {
		ds, err := p.Provider.Open(ctx, k)
		if ds != nil {
			ds = &DataStoreStub{DataStore: ds}
		}
		return ds, err
	}

	return nil, nil
}

// DataStoreStub is a test implementation of the persistence.DataStore interface.
type DataStoreStub struct {
	persistence.DataStore

	EventStoreRepositoryFunc func() eventstore.Repository
	QueueStoreRepositoryFunc func() queuestore.Repository
	BeginFunc                func(context.Context) (persistence.Transaction, error)
	CloseFunc                func() error
}

// EventStoreRepository returns the application's event store repository.
func (ds *DataStoreStub) EventStoreRepository() eventstore.Repository {
	if ds.EventStoreRepositoryFunc != nil {
		return ds.EventStoreRepositoryFunc()
	}

	if ds.DataStore != nil {
		r := ds.DataStore.EventStoreRepository()

		if r != nil {
			r = &EventStoreRepositoryStub{Repository: r}
		}

		return r
	}

	return nil
}

// QueueStoreRepository returns the application's queue store repository.
func (ds *DataStoreStub) QueueStoreRepository() queuestore.Repository {
	if ds.QueueStoreRepositoryFunc != nil {
		return ds.QueueStoreRepositoryFunc()
	}

	if ds.DataStore != nil {
		r := ds.DataStore.QueueStoreRepository()

		if r != nil {
			r = &QueueStoreRepositoryStub{Repository: r}
		}

		return r
	}

	return nil
}

// Begin starts a new transaction.
func (ds *DataStoreStub) Begin(ctx context.Context) (persistence.Transaction, error) {
	if ds.BeginFunc != nil {
		return ds.BeginFunc(ctx)
	}

	if ds.DataStore != nil {
		return ds.DataStore.Begin(ctx)
	}

	return nil, nil
}

// Close closes the data store.
func (ds *DataStoreStub) Close() error {
	if ds.CloseFunc != nil {
		return ds.CloseFunc()
	}

	if ds.DataStore != nil {
		return ds.DataStore.Close()
	}

	return nil
}
