package memory

import (
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence"
)

// dataStore is an implementation of persistence.DataStore for BoltDB.
type dataStore struct {
	stream  *Stream
	queue   *Queue
	offsets *OffsetRepository
}

func newDataStore(
	cfg configkit.RichApplication,
) *dataStore {
	return &dataStore{
		stream: &Stream{
			App: cfg.Identity(),
			Types: cfg.
				MessageTypes().
				Produced.
				FilterByRole(message.EventRole),
		},
		queue:   &Queue{},
		offsets: &OffsetRepository{},
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
	return nil
}
