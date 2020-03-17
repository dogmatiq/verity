package memory

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/persistence"
)

// dataStore is an implementation of persistence.DataStore for BoltDB.
type dataStore struct {
	AppConfig configkit.RichApplication

	once   sync.Once
	stream *Stream
}

// EventStream returns the application's event stream.
func (ds *dataStore) EventStream(context.Context) (persistence.Stream, error) {
	ds.once.Do(func() {
		ds.stream = &Stream{
			Types: ds.AppConfig.
				MessageTypes().
				Produced.
				FilterByRole(message.EventRole),
		}
	})

	return ds.stream, nil
}

// MessageQueue returns the application's queue of command and timeout messages.
func (ds *dataStore) MessageQueue(ctx context.Context) (persistence.Queue, error) {
	panic("not implemented")
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	return nil
}
