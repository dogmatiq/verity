package boltdb

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	"go.etcd.io/bbolt"
)

// dataStore is an implementation of persistence.DataStore for BoltDB.
type dataStore struct {
	App       configkit.Identity
	Marshaler marshalkit.ValueMarshaler
	DB        *bbolt.DB
	Closer    func() error

	once   sync.Once
	stream *Stream
}

// EventStream returns the event stream for the given application.
func (ds *dataStore) EventStream(_ context.Context) (persistence.Stream, error) {
	ds.once.Do(func() {
		ds.stream = &Stream{
			DB:        ds.DB,
			Marshaler: ds.Marshaler,
			BucketPath: [][]byte{
				[]byte(ds.App.Key),
				[]byte("eventstream"),
			},
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
