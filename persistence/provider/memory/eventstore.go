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

	return nil, errors.New("not implemented")
}
