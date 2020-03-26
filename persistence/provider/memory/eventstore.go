package memory

import (
	"context"

	"github.com/dogmatiq/infix/persistence/eventstore"
)

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
	if err := r.db.RLock(ctx); err != nil {
		return nil, err
	}
	defer r.db.RUnlock()

	end := eventstore.Offset(len(r.db.events))

	if q.End == 0 || end < q.End {
		q.End = end
	}

	return &eventStoreResult{
		query: q,
		db:    r.db,
	}, nil
}

// eventStoreResult is an implementation of eventstore.Result for the in-memory
// event store.
type eventStoreResult struct {
	query eventstore.Query
	db    *database
}

// Next returns the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventStoreResult) Next(ctx context.Context) (*eventstore.Event, bool, error) {
	// Bail early without acquiring the lock if we're already at the end.
	if r.query.Begin >= r.query.End {
		return nil, false, nil
	}

	// Lock the database for reads.
	if err := r.db.RLock(ctx); err != nil {
		return nil, false, err
	}
	defer r.db.RUnlock()

	filtered := len(r.query.PortableNames) > 0

	// Loop through the events in the store as per the query range.
	for r.query.Begin < r.query.End {
		// Bail if we're taking too long.
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}

		ev := r.db.events[int(r.query.Begin)]
		r.query.Begin++

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
	r.query.Begin = r.query.End
	return nil
}
