package memory

import (
	"context"
	"math"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/eventstore"
)

// SaveEvents persists events in the application's event store.
//
// It returns the next free offset in the store.
func (t *transaction) SaveEvents(
	ctx context.Context,
	envelopes []*envelopespec.Envelope,
) (eventstore.Offset, error) {
	if err := t.begin(ctx); err != nil {
		return 0, err
	}

	t.uncommitted.events = append(
		t.uncommitted.events,
		envelopes...,
	)

	return eventstore.Offset(
		len(t.ds.db.events) + len(t.uncommitted.events),
	), nil
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
	}, nil
}

// eventStoreResult is an implementation of eventstore.Result for the in-memory
// event store.
type eventStoreResult struct {
	db    *database
	query eventstore.Query
}

// Next returns the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventStoreResult) Next(ctx context.Context) (*eventstore.Event, bool, error) {
	// Lock the database for reads.
	if err := r.db.RLock(ctx); err != nil {
		return nil, false, err
	}
	defer r.db.RUnlock()

	filtered := len(r.query.PortableNames) > 0

	end := eventstore.Offset(
		len(r.db.events),
	)

	// Loop through the events in the store as per the query range.
	for r.query.MinOffset < end {
		// Bail if we're taking too long.
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}

		ev := r.db.events[int(r.query.MinOffset)]
		r.query.MinOffset++

		// Skip over anything that doesn't match the type filter.
		if filtered {
			if _, ok := r.query.PortableNames[ev.Envelope.PortableName]; !ok {
				continue
			}
		}

		// We found a match.
		return &ev, true, nil
	}

	// We've reached the end, there are no more events.
	return nil, false, nil
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	r.query.MinOffset = math.MaxUint64
	return nil
}
