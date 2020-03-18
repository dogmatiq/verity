package sql

import (
	"context"
	"database/sql"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit"
)

// dataStore is an implementation of persistence.DataStore for SQL databases.
type dataStore struct {
	appConfig     configkit.RichApplication
	marshaler     marshalkit.Marshaler
	db            *sql.DB
	driver        *driver.Driver
	streamBackoff backoff.Strategy
	closer        func() error

	once   sync.Once
	stream *Stream
}

// EventStream returns the application's event stream.
func (ds *dataStore) EventStream(context.Context) (persistence.Stream, error) {
	ds.once.Do(func() {
		ds.stream = &Stream{
			ApplicationKey:  ds.appConfig.Identity().Key,
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

	return ds.stream, nil
}

// MessageQueue returns the application's queue of command and timeout messages.
func (ds *dataStore) MessageQueue(ctx context.Context) (persistence.Queue, error) {
	panic("not implemented")
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	if ds.closer != nil {
		return ds.closer()
	}

	return nil
}
