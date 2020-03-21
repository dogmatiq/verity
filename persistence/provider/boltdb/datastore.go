package boltdb

import (
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	"go.etcd.io/bbolt"
)

// dataStore is an implementation of persistence.DataStore for BoltDB.
type dataStore struct {
	appConfig configkit.RichApplication
	marshaler marshalkit.ValueMarshaler
	db        *bbolt.DB
	closer    func() error

	once   sync.Once
	stream *Stream
}

// EventStream returns the application's event stream.
func (ds *dataStore) EventStream() eventstream.Stream {
	ds.once.Do(func() {
		ds.stream = &Stream{
			AppKey:    ds.appConfig.Identity().Key,
			DB:        ds.db,
			Marshaler: ds.marshaler,
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
