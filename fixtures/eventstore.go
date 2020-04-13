package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
)

// EventStoreRepositoryStub is a test implementation of the
// eventstore.Repository interface.
type EventStoreRepositoryStub struct {
	eventstore.Repository

	QueryEventsFunc func(context.Context, eventstore.Query) (eventstore.Result, error)
}

// QueryEvents queries events in the repository.
func (r *EventStoreRepositoryStub) QueryEvents(ctx context.Context, q eventstore.Query) (eventstore.Result, error) {
	if r.QueryEventsFunc != nil {
		return r.QueryEventsFunc(ctx, q)
	}

	if r.Repository != nil {
		return r.Repository.QueryEvents(ctx, q)
	}

	return nil, nil
}

// EventStoreResultStub is a test implementation of the eventstore.Result
// interface.
type EventStoreResultStub struct {
	eventstore.Result

	NextFunc  func(context.Context) (*eventstore.Parcel, bool, error)
	CloseFunc func() error
}

// Next returns the next event in the result.
func (r *EventStoreResultStub) Next(ctx context.Context) (*eventstore.Parcel, bool, error) {
	if r.NextFunc != nil {
		return r.NextFunc(ctx)
	}

	if r.Result != nil {
		return r.Result.Next(ctx)
	}

	return nil, false, nil
}

// Close closes the cursor.
func (r *EventStoreResultStub) Close() error {
	if r.CloseFunc != nil {
		return r.CloseFunc()
	}

	if r.Result != nil {
		return r.Result.Close()
	}

	return nil
}
