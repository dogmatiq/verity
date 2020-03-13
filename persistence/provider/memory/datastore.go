package memory

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
)

// dataStore is an implementation of persistence.DataStore for BoltDB.
type dataStore struct {
	stream Stream
}

// EventStream returns the event stream for the given application.
func (ds *dataStore) EventStream(_ context.Context) (persistence.Stream, error) {
	return &ds.stream, nil
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	return nil
}
