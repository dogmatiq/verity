package memory

import (
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
)

// dataStore is an implementation of persistence.DataStore for BoltDB.
type dataStore struct {
	stream *Stream
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
	}
}

// EventStream returns the application's event stream.
func (ds *dataStore) EventStream() eventstream.Stream {
	return ds.stream
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	return nil
}
