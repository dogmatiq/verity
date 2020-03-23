package sql

import (
	"database/sql"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit"
)

// dataStore is an implementation of persistence.DataStore for SQL databases.
type dataStore struct {
	stream *Stream
	closer func() error
}

func newDataStore(
	cfg configkit.RichApplication,
	m marshalkit.Marshaler,
	db *sql.DB,
	d *Driver,
	b backoff.Strategy,
	c func() error,
) *dataStore {
	return &dataStore{
		stream: &Stream{
			App:             cfg.Identity(),
			DB:              db,
			Driver:          d.StreamDriver,
			Marshaler:       m,
			BackoffStrategy: b,
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
