package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
)

// EventStoreRepositoryStub is a test implementation of the
// eventstore.Repository interface.
type EventStoreRepositoryStub struct {
	eventstore.Repository

	LoadEventsByTypeFunc   func(context.Context, map[string]struct{}, uint64) (eventstore.Result, error)
	LoadEventsBySourceFunc func(context.Context, string, string, string) (eventstore.Result, error)
}

// LoadEventsBySource loads the events produced by a specific handler.
func (r *EventStoreRepositoryStub) LoadEventsBySource(
	ctx context.Context,
	hk, id, d string,
) (eventstore.Result, error) {
	if r.LoadEventsBySourceFunc != nil {
		return r.LoadEventsBySourceFunc(ctx, hk, id, d)
	}

	if r.Repository != nil {
		return r.Repository.LoadEventsBySource(ctx, hk, id, d)
	}

	return nil, nil
}

// LoadEventsByType loads events that match a specific set of message types.
func (r *EventStoreRepositoryStub) LoadEventsByType(
	ctx context.Context,
	f map[string]struct{},
	o uint64,
) (eventstore.Result, error) {
	if r.LoadEventsByTypeFunc != nil {
		return r.LoadEventsByTypeFunc(ctx, f, o)
	}

	if r.Repository != nil {
		return r.Repository.LoadEventsByType(ctx, f, o)
	}

	return nil, nil
}

// EventStoreResultStub is a test implementation of the eventstore.Result
// interface.
type EventStoreResultStub struct {
	eventstore.Result

	NextFunc  func(context.Context) (*eventstore.Item, bool, error)
	CloseFunc func() error
}

// Next returns the next event in the result.
func (r *EventStoreResultStub) Next(ctx context.Context) (*eventstore.Item, bool, error) {
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
