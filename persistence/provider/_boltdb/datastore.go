package boltdb

import (
	"context"
	"errors"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	"go.etcd.io/bbolt"
)

// dataStore is an implementation of persistence.DataStore for BoltDB.
type dataStore struct {
	stream *Stream
	closer func() error
}

func newDataStore(
	cfg configkit.RichApplication,
	m marshalkit.Marshaler,
	db *bbolt.DB,
	c func() error,
) *dataStore {
	return &dataStore{
		stream: &Stream{
			App:       cfg.Identity(),
			DB:        db,
			Marshaler: m,
			Types: cfg.
				MessageTypes().
				Produced.
				FilterByRole(message.EventRole),
		},
		closer: c,
	}
}

// EventRepository returns the application's event repository.
func (ds *dataStore) EventRepository() persistence.EventRepository {
	panic("not implemented")
}

// Begin starts a new transaction.
func (ds *dataStore) Begin(ctx context.Context) (persistence.Transaction, error) {
	return nil, errors.New("not implemented")
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	if ds.closer != nil {
		return ds.closer()
	}

	return nil
}
