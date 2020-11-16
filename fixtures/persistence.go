package fixtures

import (
	"context"

	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/persistence/provider/memory"
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

	LoadAggregateMetaDataFunc func(context.Context, string, string) (persistence.AggregateMetaData, error)
	LoadEventsByTypeFunc      func(context.Context, map[string]struct{}, uint64) (persistence.EventResult, error)
	LoadEventsBySourceFunc    func(context.Context, string, string, string) (persistence.EventResult, error)
	LoadOffsetFunc            func(context.Context, string) (uint64, error)
	LoadProcessInstanceFunc   func(context.Context, string, string) (persistence.ProcessInstance, error)
	LoadQueueMessagesFunc     func(context.Context, int) ([]persistence.QueueMessage, error)
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
) (persistence.AggregateMetaData, error) {
	if ds.LoadAggregateMetaDataFunc != nil {
		return ds.LoadAggregateMetaDataFunc(ctx, hk, id)
	}

	if ds.DataStore != nil {
		return ds.DataStore.LoadAggregateMetaData(ctx, hk, id)
	}

	return persistence.AggregateMetaData{}, nil
}

// LoadEventsBySource loads the events produced by a specific handler.
func (ds *DataStoreStub) LoadEventsBySource(
	ctx context.Context,
	hk, id, d string,
) (persistence.EventResult, error) {
	if ds.LoadEventsBySourceFunc != nil {
		return ds.LoadEventsBySourceFunc(ctx, hk, id, d)
	}

	if ds.DataStore != nil {
		return ds.DataStore.LoadEventsBySource(ctx, hk, id, d)
	}

	return nil, nil
}

// LoadEventsByType loads events that match a specific set of message types.
func (ds *DataStoreStub) LoadEventsByType(
	ctx context.Context,
	f map[string]struct{},
	o uint64,
) (persistence.EventResult, error) {
	if ds.LoadEventsByTypeFunc != nil {
		return ds.LoadEventsByTypeFunc(ctx, f, o)
	}

	if ds.DataStore != nil {
		return ds.DataStore.LoadEventsByType(ctx, f, o)
	}

	return nil, nil
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

// LoadProcessInstance loads a process instance.
func (ds *DataStoreStub) LoadProcessInstance(
	ctx context.Context,
	hk, id string,
) (persistence.ProcessInstance, error) {
	if ds.LoadProcessInstanceFunc != nil {
		return ds.LoadProcessInstanceFunc(ctx, hk, id)
	}

	if ds.DataStore != nil {
		return ds.DataStore.LoadProcessInstance(ctx, hk, id)
	}

	return persistence.ProcessInstance{}, nil
}

// LoadQueueMessages loads the next n messages from the queue.
func (ds *DataStoreStub) LoadQueueMessages(
	ctx context.Context,
	n int,
) ([]persistence.QueueMessage, error) {
	if ds.LoadQueueMessagesFunc != nil {
		return ds.LoadQueueMessagesFunc(ctx, n)
	}

	if ds.DataStore != nil {
		return ds.DataStore.LoadQueueMessages(ctx, n)
	}

	return nil, nil
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

// EventResultStub is a test implementation of the persistence.EventResult
// interface.
type EventResultStub struct {
	persistence.EventResult

	NextFunc  func(context.Context) (persistence.Event, bool, error)
	CloseFunc func() error
}

// Next returns the next event in the result.
func (r *EventResultStub) Next(ctx context.Context) (persistence.Event, bool, error) {
	if r.NextFunc != nil {
		return r.NextFunc(ctx)
	}

	if r.EventResult != nil {
		return r.EventResult.Next(ctx)
	}

	return persistence.Event{}, false, nil
}

// Close closes the cursor.
func (r *EventResultStub) Close() error {
	if r.CloseFunc != nil {
		return r.CloseFunc()
	}

	if r.EventResult != nil {
		return r.EventResult.Close()
	}

	return nil
}
