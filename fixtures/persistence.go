package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
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

	AggregateStoreRepositoryFunc func() aggregatestore.Repository
	OffsetStoreRepositoryFunc    func() offsetstore.Repository
	EventStoreRepositoryFunc     func() eventstore.Repository
	QueueStoreRepositoryFunc     func() queuestore.Repository
	PersistFunc                  func(context.Context, persistence.Batch) (persistence.Result, error)
	CloseFunc                    func() error
}

// NewDataStoreStub returns a new data-store stub that uses an in-memory
// persistence provider.
func NewDataStoreStub() *DataStoreStub {
	p := &ProviderStub{
		Provider: &memory.Provider{},
	}

	ds, err := p.Open(context.Background(), "<app-key>")
	if err != nil {
		panic(err)
	}

	return ds.(*DataStoreStub)
}

// AggregateStoreRepository returns the application's aggregate store
// repository.
func (ds *DataStoreStub) AggregateStoreRepository() aggregatestore.Repository {
	if ds.EventStoreRepositoryFunc != nil {
		return ds.AggregateStoreRepositoryFunc()
	}

	if ds.DataStore != nil {
		r := ds.DataStore.AggregateStoreRepository()

		if r != nil {
			r = &AggregateStoreRepositoryStub{Repository: r}
		}

		return r
	}

	return nil
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

// OffsetStoreRepository returns the application's offset store repository.
func (ds *DataStoreStub) OffsetStoreRepository() offsetstore.Repository {
	if ds.OffsetStoreRepositoryFunc != nil {
		return ds.OffsetStoreRepositoryFunc()
	}

	if ds.DataStore != nil {
		r := ds.DataStore.OffsetStoreRepository()

		if r != nil {
			r = &OffsetStoreRepositoryStub{Repository: r}
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

// Persist commits a batch of operations atomically.
func (ds *DataStoreStub) Persist(ctx context.Context, b persistence.Batch) (persistence.Result, error) {
	if ds.PersistFunc != nil {
		return ds.PersistFunc(ctx, b)
	}

	if ds.DataStore != nil {
		return ds.DataStore.Persist(ctx, b)
	}

	return persistence.Result{}, nil
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
