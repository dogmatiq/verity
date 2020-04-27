package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
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
	BeginFunc                    func(context.Context) (persistence.Transaction, error)
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

// Begin starts a new transaction.
func (ds *DataStoreStub) Begin(ctx context.Context) (persistence.Transaction, error) {
	if ds.BeginFunc != nil {
		return ds.BeginFunc(ctx)
	}

	if ds.DataStore != nil {
		tx, err := ds.DataStore.Begin(ctx)

		if tx != nil {
			tx = &TransactionStub{Transaction: tx}
		}

		return tx, err
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

// TransactionStub is a test implementation of the persistence.Transaction
// interface.
type TransactionStub struct {
	persistence.Transaction

	SaveAggregateMetaDataFunc  func(context.Context, *aggregatestore.MetaData) error
	SaveEventFunc              func(context.Context, *envelopespec.Envelope) (uint64, error)
	SaveOffsetFunc             func(ctx context.Context, ak string, c, n uint64) error
	SaveMessageToQueueFunc     func(context.Context, *queuestore.Item) error
	RemoveMessageFromQueueFunc func(context.Context, *queuestore.Item) error

	CommitFunc   func(context.Context) error
	RollbackFunc func() error
}

// SaveAggregateMetaData persists meta-data about an aggregate instance.
func (t *TransactionStub) SaveAggregateMetaData(ctx context.Context, md *aggregatestore.MetaData) error {
	if t.SaveAggregateMetaDataFunc != nil {
		return t.SaveAggregateMetaDataFunc(ctx, md)
	}

	if t.Transaction != nil {
		return t.Transaction.SaveAggregateMetaData(ctx, md)
	}

	return nil
}

// SaveEvent persists an event in the application's event store.
func (t *TransactionStub) SaveEvent(ctx context.Context, env *envelopespec.Envelope) (uint64, error) {
	if t.SaveEventFunc != nil {
		return t.SaveEventFunc(ctx, env)
	}

	if t.Transaction != nil {
		return t.Transaction.SaveEvent(ctx, env)
	}

	return 0, nil
}

// SaveOffset persists the "next" offset to be consumed for a specific
// application.
func (t *TransactionStub) SaveOffset(
	ctx context.Context,
	ak string,
	c, n uint64,
) error {
	if t.SaveOffsetFunc != nil {
		return t.SaveOffsetFunc(ctx, ak, c, n)
	}

	if t.Transaction != nil {
		return t.Transaction.SaveOffset(ctx, ak, c, n)
	}

	return nil
}

// SaveMessageToQueue persists a message to the application's message queue.
func (t *TransactionStub) SaveMessageToQueue(ctx context.Context, i *queuestore.Item) error {
	if t.SaveMessageToQueueFunc != nil {
		return t.SaveMessageToQueueFunc(ctx, i)
	}

	if t.Transaction != nil {
		return t.Transaction.SaveMessageToQueue(ctx, i)
	}

	return nil
}

// RemoveMessageFromQueue removes a specific message from the application's
// message queue.
func (t *TransactionStub) RemoveMessageFromQueue(ctx context.Context, i *queuestore.Item) error {
	if t.RemoveMessageFromQueueFunc != nil {
		return t.RemoveMessageFromQueueFunc(ctx, i)
	}

	if t.Transaction != nil {
		return t.Transaction.RemoveMessageFromQueue(ctx, i)
	}

	return nil
}

// Commit applies the changes from the transaction.
func (t *TransactionStub) Commit(ctx context.Context) error {
	if t.CommitFunc != nil {
		return t.CommitFunc(ctx)
	}

	if t.Transaction != nil {
		return t.Transaction.Commit(ctx)
	}

	return nil
}

// Rollback aborts the transaction.
func (t *TransactionStub) Rollback() error {
	if t.RollbackFunc != nil {
		return t.RollbackFunc()
	}

	if t.Transaction != nil {
		return t.Transaction.Rollback()
	}

	return nil
}
