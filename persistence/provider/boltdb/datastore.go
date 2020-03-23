package boltdb

import (
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
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

// EventStream returns the application's event stream.
func (ds *dataStore) EventStream() eventstream.Stream {
	return ds.stream
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	if ds.closer != nil {
		return ds.closer()
	}

	return nil
}
