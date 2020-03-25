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
	if q.Types != nil && q.Types.Len() == 0 {
		panic("q.Types must be nil or otherwise contain at least one event type")
	}

	db, err := r.ds.database()
	if err != nil {
		return nil, err
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
		n := len(r.db.events)
		o := int(r.query.NotBefore)

		for o < n {
			ev := r.db.events[o]
			o++

			if r.query.Types == nil || r.query.Types.Has(ev.Type) {
				r.query.NotBefore = eventstore.Offset(o)
				r.event = &ev.Event
				return true
			}
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
