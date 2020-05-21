package memory

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
)

// NextEventOffset returns the next "unused" offset within the store.
func (ds *dataStore) NextEventOffset(
	ctx context.Context,
) (uint64, error) {
	ds.db.mutex.RLock()
	defer ds.db.mutex.RUnlock()

	return uint64(len(ds.db.event.items)), nil
}

// LoadEventsByType loads events that match a specific set of message types.
//
// f is the set of message types to include in the result. The keys of f are
// the "portable type name" produced when the events are marshaled.
//
// o specifies the (inclusive) lower-bound of the offset range to include in
// the results.
func (ds *dataStore) LoadEventsByType(
	ctx context.Context,
	f map[string]struct{},
	o uint64,
) (eventstore.Result, error) {
	return &eventResult{
		db:    ds.db,
		index: int(o),
		pred: func(item *eventstore.Item) bool {
			_, ok := f[item.Envelope.PortableName]
			return ok
		},
	}, nil
}

// LoadEventsBySource loads the events produced by a specific handler.
//
// hk is the handler's identity key.
//
// id is the instance ID, which must be empty if the handler type does not
// use instances.
//
// m is ID of a "barrier" message. If supplied, the results are limited to
// events with higher offsets than the barrier message. If the message
// cannot be found, UnknownMessageError is returned.
func (ds *dataStore) LoadEventsBySource(
	ctx context.Context,
	hk, id, m string,
) (eventstore.Result, error) {
	var o uint64

	if m != "" {
		ds.db.mutex.RLock()
		defer ds.db.mutex.RUnlock()

		var ok bool
		o, ok = ds.db.event.offsets[m]
		if !ok {
			return nil, persistence.UnknownMessageError{
				MessageID: m,
			}
		}

		// Increment the offset to select all messages after the barrier
		// message exclusively.
		o++
	}

	return &eventResult{
		db:    ds.db,
		index: int(o),
		pred: func(i *eventstore.Item) bool {
			return hk == i.Envelope.MetaData.Source.Handler.Key &&
				id == i.Envelope.MetaData.Source.InstanceId
		},
	}, nil
}

// eventResult is an implementation of eventstore.Result for the in-memory
// event store.
type eventResult struct {
	db    *database
	pred  func(*eventstore.Item) bool
	index int
}

// Next returns the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventResult) Next(
	ctx context.Context,
) (*eventstore.Item, bool, error) {
	r.db.mutex.RLock()

	// We only have to hold the mutex long enough to make the slice that is our
	// "view" of the items. The individual items are never modified and so they
	// are safe to read concurrently without synchronization. Any future append
	// will either add elements to the tail of the underlying array, or allocate
	// a new array, neither of which we will see via this slice.
	var items []eventstore.Item
	if len(r.db.event.items) > r.index {
		items = r.db.event.items[r.index:]
	}

	r.db.mutex.RUnlock()

	// Iterate through the items looking for a match for the query.
	for i, item := range items {
		if r.pred(&item) {
			r.index += i + 1

			// Clone the envelope on the way out so inadvertent manipulation of
			// the envelope by the caller does not affect the data in the
			// database.
			item.Envelope = cloneEnvelope(item.Envelope)

			return &item, true, nil
		}
	}

	r.index += len(items) + 1

	return nil, false, nil
}

// Close closes the cursor.
func (r *eventResult) Close() error {
	return nil
}

// eventDatabase contains data that is committed to the event store.
type eventDatabase struct {
	items   []eventstore.Item
	offsets map[string]uint64
}

// VisitSaveEvent returns an error if a "SaveEvent" operation can not be applied
// to the database.
func (v *validator) VisitSaveEvent(
	_ context.Context,
	op persistence.SaveEvent,
) error {
	return nil
}

// VisitSaveEvent applies the changes in a "SaveEvent" operation to the
// database.
func (c *committer) VisitSaveEvent(
	_ context.Context,
	op persistence.SaveEvent,
) error {
	offset := uint64(len(c.db.event.items))

	c.db.event.items = append(
		c.db.event.items,
		eventstore.Item{
			Offset:   offset,
			Envelope: cloneEnvelope(op.Envelope),
		},
	)

	if c.db.event.offsets == nil {
		c.db.event.offsets = map[string]uint64{}
	}

	c.db.event.offsets[op.Envelope.MetaData.MessageId] = offset

	if c.result.EventOffsets == nil {
		c.result.EventOffsets = map[string]uint64{}
	}

	c.result.EventOffsets[op.Envelope.MetaData.MessageId] = offset

	return nil
}
