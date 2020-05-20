package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
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

	LoadAggregateMetaDataFunc func(context.Context, string, string) (*persistence.AggregateMetaData, error)
	EventStoreRepositoryFunc  func() eventstore.Repository
	LoadOffsetFunc            func(context.Context, string) (uint64, error)
	QueueStoreRepositoryFunc  func() queuestore.Repository
	PersistFunc               func(context.Context, persistence.Batch) (persistence.Result, error)
	CloseFunc                 func() error
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

// LoadAggregateMetaData loads the meta-data for an aggregate instance.
func (ds *DataStoreStub) LoadAggregateMetaData(
	ctx context.Context,
	hk, id string,
) (*persistence.AggregateMetaData, error) {
	if ds.LoadAggregateMetaDataFunc != nil {
		return ds.LoadAggregateMetaDataFunc(ctx, hk, id)
	}

	if ds.DataStore != nil {
		return ds.DataStore.LoadAggregateMetaData(ctx, hk, id)
	}

	return nil, nil
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

// LoadOffset loads the offset associated with a specific application.
func (ds *DataStoreStub) LoadOffset(
	ctx context.Context,
	ak string,
) (uint64, error) {
	if ds.LoadOffsetFunc != nil {
		return ds.LoadOffsetFunc(ctx, ak)
	}

	if ds.DataStore != nil {
		return ds.DataStore.LoadOffset(ctx, ak)
	}

	return 0, nil
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
