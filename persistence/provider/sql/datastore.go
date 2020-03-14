package sql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit"
)

// dataStore is an implementation of persistence.DataStore for SQL databases.
type dataStore struct {
	App           configkit.Identity
	Marshaler     marshalkit.Marshaler
	DB            *sql.DB
	Driver        *driver.Driver
	StreamBackoff backoff.Strategy
	Closer        func() error
}

// EventStream returns the event stream for the given application.
func (ds *dataStore) EventStream(context.Context) (persistence.Stream, error) {
	return &Stream{
		ApplicationKey:  ds.App.Key,
		DB:              ds.DB,
		Driver:          ds.Driver.StreamDriver,
		Marshaler:       ds.Marshaler,
		BackoffStrategy: ds.StreamBackoff,
	}, nil
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	if ds.Closer != nil {
		return ds.Closer()
	}

	return nil
}
