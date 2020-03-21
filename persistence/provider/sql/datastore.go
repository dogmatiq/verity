package sql

import (
	"database/sql"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit"
)

// dataStore is an implementation of persistence.DataStore for SQL databases.
type dataStore struct {
	appConfig     configkit.RichApplication
	marshaler     marshalkit.Marshaler
	db            *sql.DB
	driver        *Driver
	streamBackoff backoff.Strategy
	closer        func() error

	once   sync.Once
	stream *Stream
}

// EventStream returns the application's event stream.
func (ds *dataStore) EventStream() eventstream.Stream {
	ds.once.Do(func() {
		ds.stream = &Stream{
			App:             ds.appConfig.Identity(),
			DB:              ds.db,
			Driver:          ds.driver.StreamDriver,
			Marshaler:       ds.marshaler,
			BackoffStrategy: ds.streamBackoff,
			Types: ds.appConfig.
				MessageTypes().
				Produced.
				FilterByRole(message.EventRole),
		}
	})

	return ds.stream
}

// MessageQueue returns the application's queue of command and timeout messages.
func (ds *dataStore) MessageQueue() persistence.Queue {
	panic("not implemented")
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	if ds.closer != nil {
		return ds.closer()
	}

	return nil
}
