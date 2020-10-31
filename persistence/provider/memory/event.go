package memory

import (
	"context"

	"github.com/dogmatiq/envelopespec"
	"github.com/dogmatiq/infix/persistence"
)

// NextEventOffset returns the next "unused" offset.
func (ds *dataStore) NextEventOffset(
	ctx context.Context,
) (uint64, error) {
	ds.db.mutex.RLock()
	defer ds.db.mutex.RUnlock()

	return uint64(len(ds.db.event.events)), nil
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
) (persistence.EventResult, error) {
	return &eventResult{
		db:    ds.db,
		index: int(o),
		pred: func(env *envelopespec.Envelope) bool {
			_, ok := f[env.GetPortableName()]
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
) (persistence.EventResult, error) {
	var offset uint64

	if m != "" {
		ds.db.mutex.RLock()
		defer ds.db.mutex.RUnlock()

		o, ok := ds.db.event.offsets[m]
		if !ok {
			return nil, persistence.UnknownMessageError{
				MessageID: m,
			}
		}

		offset = o + 1 // start with the message AFTER the barrier message.
	}

	return &eventResult{
		db:    ds.db,
		index: int(offset),
		pred: func(env *envelopespec.Envelope) bool {
			return env.GetSourceHandler().GetKey() == hk &&
				env.GetSourceInstanceId() == id
		},
	}, nil
}

// eventResult is an implementation of persistence.EventResult for the in-memory
// provider.
type eventResult struct {
	db    *database
	index int
	pred  func(*envelopespec.Envelope) bool
}

// Next returns the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventResult) Next(
	ctx context.Context,
) (persistence.Event, bool, error) {
	// We only have to hold the mutex long enough to make the slice that is our
	// "view" of the events. The individual events are never modified and so
	// they are safe to read concurrently without synchronization. Any future
	// append will either add elements to the tail of the underlying array, or
	// allocate a new array, neither of which we will see via this slice.
	var events []persistence.Event
	r.db.mutex.RLock()
	if len(r.db.event.events) > r.index {
		events = r.db.event.events[r.index:]
	}
	r.db.mutex.RUnlock()

	// Iterate through the events looking for a predicate match.
	for i, ev := range events {
		if r.pred(ev.Envelope) {
			r.index += i + 1

			// Clone the envelope on the way out so inadvertent manipulation of
			// the envelope by the caller does not affect the data in the
			// database.
			ev.Envelope = cloneEnvelope(ev.Envelope)

			return ev, true, nil
		}
	}

	r.index += len(events) + 1

	return persistence.Event{}, false, nil
}

// Close closes the cursor.
func (r *eventResult) Close() error {
	return nil
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
	offset := c.db.event.save(op.Envelope)

	if c.result.EventOffsets == nil {
		c.result.EventOffsets = map[string]uint64{}
	}

	c.result.EventOffsets[op.Envelope.GetMessageId()] = offset

	return nil
}

// eventDatabase contains event related data.
type eventDatabase struct {
	events  []persistence.Event
	offsets map[string]uint64
}

// save appends the event in env to the database.
func (db *eventDatabase) save(env *envelopespec.Envelope) uint64 {
	offset := uint64(len(db.events))

	ev := persistence.Event{
		Offset:   offset,
		Envelope: cloneEnvelope(env),
	}

	db.events = append(db.events, ev)

	if db.offsets == nil {
		db.offsets = map[string]uint64{}
	}

	db.offsets[ev.ID()] = offset

	return offset
}
