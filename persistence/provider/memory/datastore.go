package memory

import (
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence"
)

// dataStore is an implementation of persistence.DataStore for BoltDB.
type dataStore struct {
	appConfig configkit.RichApplication

	once   sync.Once
	stream *Stream
	queue  *Queue
}

// EventStream returns the application's event stream.
func (ds *dataStore) EventStream() eventstream.Stream {
	ds.init()
	return ds.stream
}

// MessageQueue returns the application's queue of command and timeout messages.
func (ds *dataStore) MessageQueue() persistence.Queue {
	ds.init()
	return ds.queue
}

func (ds *dataStore) init() {
	ds.once.Do(func() {
		ds.stream = &Stream{
			Types: ds.appConfig.
				MessageTypes().
				Produced.
				FilterByRole(message.EventRole),
		}

		ds.queue = &Queue{}
	})
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	return nil
}
