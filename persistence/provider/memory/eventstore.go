package memory

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/persistence/eventstore"
)

type eventStoreRepository struct {
	ds *dataStore
}

// QueryEvents queries events in the repository.
//
// types is the set of event types that should be returned by Cursor.Next().
// Any other event types are ignored.
func (r *eventStoreRepository) QueryEvents(
	ctx context.Context,
	q eventstore.Query,
) (eventstore.ResultSet, error) {
	_, err := r.ds.get()
	if err != nil {
		return nil, err
	}

	return &eventStoreResultSet{}, nil
}

type eventStoreResultSet struct {
}

// Next advances to the next event in the result set.
//
// It returns false if the end of the result set is reached.
func (r *eventStoreResultSet) Next() bool {
	return false
}

// Next returns current event in the result set.
func (r *eventStoreResultSet) Get(ctx context.Context) (*eventstore.Event, error) {
	return nil, errors.New("not implemented")
}

// Close closes the cursor.
func (r *eventStoreResultSet) Close() error {
	return nil
}
