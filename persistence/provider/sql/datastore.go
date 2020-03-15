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
	AppConfig     configkit.RichApplication
	Marshaler     marshalkit.Marshaler
	DB            *sql.DB
	Driver        *driver.Driver
	StreamBackoff backoff.Strategy
	Closer        func() error

	once   sync.Once
	stream *Stream
}

// EventStream returns the event stream for the given application.
func (ds *dataStore) EventStream(context.Context) (persistence.Stream, error) {
	ds.once.Do(func() {
		ds.stream = &Stream{
			ApplicationKey:  ds.AppConfig.Identity().Key,
			DB:              ds.DB,
			Driver:          ds.Driver.StreamDriver,
			Marshaler:       ds.Marshaler,
			BackoffStrategy: ds.StreamBackoff,
			Types: ds.AppConfig.
				MessageTypes().
				Produced.
				FilterByRole(message.EventRole),
		}
	})

	return ds.stream, nil
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	if ds.Closer != nil {
		return ds.Closer()
	}

	return nil
}
