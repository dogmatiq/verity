package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
)

// EventStoreRepositoryStub is a test implementation of the
// eventstore.Repository interface.
type EventStoreRepositoryStub struct {
	eventstore.Repository

	LoadEventsByTypeFunc   func(context.Context, map[string]struct{}, uint64) (persistence.EventResult, error)
	LoadEventsBySourceFunc func(context.Context, string, string, string) (persistence.EventResult, error)
}

// LoadEventsBySource loads the events produced by a specific handler.
func (r *EventStoreRepositoryStub) LoadEventsBySource(
	ctx context.Context,
	hk, id, d string,
) (persistence.EventResult, error) {
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
) (persistence.EventResult, error) {
	if r.LoadEventsByTypeFunc != nil {
		return r.LoadEventsByTypeFunc(ctx, f, o)
	}

	if r.Repository != nil {
		return r.Repository.LoadEventsByType(ctx, f, o)
	}

	return nil, nil
}
