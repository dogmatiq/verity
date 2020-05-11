package memory

import (
	"context"
	"fmt"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
)

// SaveEvent persists an event in the application's event store.
//
// It returns the event's offset.
func (t *transaction) SaveEvent(
	ctx context.Context,
	env *envelopespec.Envelope,
) (uint64, error) {
	if err := t.begin(ctx); err != nil {
		return 0, err
	}

	return t.event.stageSave(&t.ds.db.event, env), nil
}

// eventStoreRepository is an implementation of eventstore.Repository that
// stores events in memory.
type eventStoreRepository struct {
	db *database
}

// NextEventOffset returns the next "unused" offset within the store.
func (r *eventStoreRepository) NextEventOffset(
	ctx context.Context,
) (uint64, error) {
	if err := r.db.RLock(ctx); err != nil {
		return 0, err
	}

	next := len(r.db.event.items)
	r.db.RUnlock()

	return uint64(next), nil
}

// LoadEventsBySource loads the events produced by the specified source with
// the given handler key and id.
//
// d is the optional parameter, it represents ID of the message that was
// recorded when the instance was last destroyed.
func (r *eventStoreRepository) LoadEventsBySource(
	ctx context.Context,
	hk, id, d string,
) (eventstore.Result, error) {
	var o uint64

	if d != "" {
		var ok bool
		o, ok = r.db.event.messageIDs[d]
		if !ok {
			return nil, fmt.Errorf(
				"message with id %s is not found",
				d,
			)
		}

		o = o + 1
	}

	return &eventStoreResult{
		db: r.db,
		pred: func(i *eventstore.Item) bool {
			return hk == i.Envelope.MetaData.Source.Handler.Key &&
				id == i.Envelope.MetaData.Source.InstanceId
		},
		index: int(o),
	}, nil
}

// QueryEvents queries events in the repository.
func (r *eventStoreRepository) QueryEvents(
	ctx context.Context,
	q eventstore.Query,
) (eventstore.Result, error) {
	return &eventStoreResult{
		db:    r.db,
		pred:  q.IsMatch,
		index: int(q.MinOffset),
	}, nil
}

// eventStoreResult is an implementation of eventstore.Result for the in-memory
// event store.
type eventStoreResult struct {
	db    *database
	pred  func(i *eventstore.Item) bool
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
	items := r.db.event.view(r.index)
	r.db.RUnlock()

	// Iterate through the items looking for a match for the query.
	for i, item := range items {
		if r.pred(item) {
			r.index += i + 1

			// Clone item on the way out so inadvertent manipulation of the item
			// by the caller does not affect the data in the event store.
			return cloneEventStoreItem(item), true, nil
		}
	}

	r.index += len(items) + 1

	return nil, false, nil
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	return nil
}

// eventStoreChangeSet contains modifications to the event store that have
// been performed within a transaction but not yet committed.
type eventStoreChangeSet struct {
	items []*eventstore.Item
}

// stageSave adds a "SaveEvent" operation to the change-set.
func (cs *eventStoreChangeSet) stageSave(
	db *eventStoreDatabase,
	env *envelopespec.Envelope,
) uint64 {
	// Find the offset for the new event based on what's already in the database
	// and the new events in this change-set.
	next := uint64(
		len(db.items) + len(cs.items),
	)

	item := &eventstore.Item{
		Offset:   next,
		Envelope: cloneEnvelope(env),
	}

	cs.items = append(cs.items, item)
	return next
}

// eventStoreDatabase contains data that is committed to the event store.
type eventStoreDatabase struct {
	items      []*eventstore.Item
	messageIDs map[string]uint64
}

// apply updates the database to include the changes in cs.
func (db *eventStoreDatabase) apply(cs *eventStoreChangeSet) {
	db.items = append(db.items, cs.items...)

	if db.messageIDs == nil {
		db.messageIDs = make(map[string]uint64)
	}

	for _, item := range cs.items {
		db.messageIDs[item.Envelope.MetaData.MessageId] = item.Offset
	}
}

// view returns a slice of the items starting at a given offset.
func (db *eventStoreDatabase) view(start int) []*eventstore.Item {
	if len(db.items) <= start {
		return nil
	}

	return db.items[start:]
}

// clone returns a deep clone of an eventstore.Item.
func cloneEventStoreItem(i *eventstore.Item) *eventstore.Item {
	clone := *i
	clone.Envelope = cloneEnvelope(clone.Envelope)
	return &clone
}
