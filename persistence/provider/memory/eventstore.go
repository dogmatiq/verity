package memory

import (
	"context"
	"math"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
)

// eventStoreCommitted contains data committed to the event-store.
type eventStoreCommitted struct {
	items []*eventstore.Item
}

func (c *eventStoreCommitted) apply(u *eventStoreUncommitted) {
	c.items = append(c.items, u.items...)
}

// eventStoreUncommitted contains modifications to the event store that have
// been performed within a transaction but not yet committed.
type eventStoreUncommitted struct {
	items []*eventstore.Item
}

func (u *eventStoreUncommitted) saveEvent(
	c *eventStoreCommitted,
	env *envelopespec.Envelope,
) eventstore.Offset {
	next := eventstore.Offset(
		len(c.items) + len(u.items),
	)

	item := &eventstore.Item{
		Offset:   next,
		Envelope: cloneEnvelope(env),
	}

	u.items = append(u.items, item)

	return next
}

// SaveEvent persists an event in the application's event store.
//
// It returns the event's offset.
func (t *transaction) SaveEvent(
	ctx context.Context,
	env *envelopespec.Envelope,
) (eventstore.Offset, error) {
	if err := t.begin(ctx); err != nil {
		return 0, err
	}

	return t.eventStore.saveEvent(&t.ds.db.eventStore, env), nil
}

// eventStoreRepository is an implementation of eventstore.Repository that
// stores events in memory.
type eventStoreRepository struct {
	db *database
}

// QueryEvents queries events in the repository.
func (r *eventStoreRepository) QueryEvents(
	ctx context.Context,
	q eventstore.Query,
) (eventstore.Result, error) {
	return &eventStoreResult{
		db:    r.db,
		query: q,
		index: int(q.MinOffset),
	}, nil
}

// eventStoreResult is an implementation of eventstore.Result for the in-memory
// event store.
type eventStoreResult struct {
	db    *database
	query eventstore.Query
	index int
}

// Next returns the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventStoreResult) Next(
	ctx context.Context,
) (*eventstore.Item, bool, error) {
	if err := r.db.RLock(ctx); err != nil {
		return nil, false, err
	}
	defer r.db.RUnlock()

	for r.index < len(r.db.eventStore.items) {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}

		i := r.db.eventStore.items[r.index]
		r.index++

		if r.query.IsMatch(i) {
			// Clone item on the way out so inadvertent manipulation does not
			// affect the data in the data-store.
			return cloneEventStoreItem(i), true, nil
		}
	}

	return nil, false, nil
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	r.query.MinOffset = math.MaxUint64
	return nil
}
