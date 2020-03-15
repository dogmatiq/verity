package boltdb

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	"go.etcd.io/bbolt"
)

// dataStore is an implementation of persistence.DataStore for BoltDB.
type dataStore struct {
	AppConfig configkit.RichApplication
	Marshaler marshalkit.ValueMarshaler
	DB        *bbolt.DB
	Closer    func() error

	once   sync.Once
	stream *Stream
}

// EventStream returns the event stream for the given application.
func (ds *dataStore) EventStream(context.Context) (persistence.Stream, error) {
	ds.once.Do(func() {
		// This can be cleaned up with a single function.
		// See https://github.com/dogmatiq/configkit/issues/62
		types := message.TypeSet{}

		for t, r := range ds.AppConfig.MessageTypes().Produced {
			if r == message.EventRole {
				types.Add(t)
			}
		}

		ds.stream = &Stream{
			DB:        ds.DB,
			Types:     types,
			Marshaler: ds.Marshaler,
			BucketPath: [][]byte{
				[]byte(ds.AppConfig.Identity().Key),
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
