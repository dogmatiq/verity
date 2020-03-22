package sql

import (
	"database/sql"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit"
)

// dataStore is an implementation of persistence.DataStore for SQL databases.
type dataStore struct {
	stream  *Stream
	queue   *Queue
	offsets *OffsetRepository
	closer  func() error
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
