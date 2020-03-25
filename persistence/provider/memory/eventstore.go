package memory

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/persistence/eventstore"
)

// eventStoreRepository is an implementation of eventstore.Repository that
// stores events in memory.
type eventStoreRepository struct {
	ds *dataStore
}

// QueryEvents queries events in the repository.
func (r *eventStoreRepository) QueryEvents(
	ctx context.Context,
	q eventstore.Query,
) (eventstore.Result, error) {
	db, err := r.ds.database()
	if err != nil {
		return nil, err
	}

	db.m.RLock()
	defer db.m.RUnlock()

	end := eventstore.Offset(len(db.events))

	if q.End == 0 || end < q.End {
		q.End = end
	}

	return &eventStoreResult{
		query: q,
		db:    db,
	}, nil
}

// eventStoreResult is an implementation of eventstore.Result for the in-memory
// event store.
type eventStoreResult struct {
	query eventstore.Query
	db    *database
	event *eventstore.Event
	done  bool
}

// Next advances to the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventStoreResult) Next() bool {
	r.db.m.RLock()
	defer r.db.m.RUnlock()

	if !r.done {
		filtered := len(r.query.PortableNames) > 0

		for r.query.Begin < r.query.End {
			ev := r.db.events[int(r.query.Begin)]
			r.query.Begin++

			if filtered {
				if _, ok := r.query.PortableNames[ev.Envelope.PortableName]; !ok {
					continue
				}
			}

			r.event = &ev

			return true
		}
	}

	r.Close()

	return false
}

// Next returns current event in the result.
func (r *eventStoreResult) Get(ctx context.Context) (*eventstore.Event, error) {
	if r.done {
		return nil, errors.New("no more results")
	}

	if r.event == nil {
		panic("Next() must be called before calling Get()")
	}

	return r.event, nil
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	r.db = nil
	r.event = nil
	r.done = true

	return nil
}
