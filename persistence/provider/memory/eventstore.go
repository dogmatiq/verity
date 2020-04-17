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

func (c *eventStoreCommitted) view(start int) []*eventstore.Item {
	if len(c.items) <= start {
		return nil
	}

	return c.items[start:]
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

	// We only have to hold the mutex long enough to make the slice that is our
	// "view" of the items. The individual items are never modified and so they
	// are safe to read concurrently without synchronization. Any future append
	// will either add elements to the tail of the underlying array, or allocate
	// a new array, neither of which we will see via this slice.
	items := r.db.eventStore.view(r.index)
	r.db.RUnlock()

	for i, it := range items {
		if r.query.IsMatch(it) {
			r.index += i + 1

			// Clone item on the way out so inadvertent manipulation does not
			// affect the data in the data-store.
			return cloneEventStoreItem(it), true, nil
		}
	}

	r.index += len(items) + 1

	return nil, false, nil
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	r.query.MinOffset = math.MaxUint64
	return nil
}
