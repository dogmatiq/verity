package boltdb

import (
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	"go.etcd.io/bbolt"
)

// dataStore is an implementation of persistence.DataStore for BoltDB.
type dataStore struct {
	stream  *Stream
	queue   *Queue
	offsets *OffsetRepository
	closer  func() error
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
		queue:   &Queue{},
		offsets: &OffsetRepository{},
		closer:  c,
	}
}

// EventStream returns the application's event stream.
func (ds *dataStore) EventStream() eventstream.Stream {
	return ds.stream
}

// MessageQueue returns the application's queue of command and timeout messages.
func (ds *dataStore) MessageQueue() persistence.Queue {
	return ds.queue
}

// OffsetRepository returns the repository that stores the "progress" of
// message handlers through the event streams they consume.
func (ds *dataStore) OffsetRepository() persistence.OffsetRepository {
	return ds.offsets
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	if ds.closer != nil {
		return ds.closer()
	}

	return nil
}
