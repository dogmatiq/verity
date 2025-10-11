package persistedstream

import (
	"context"

	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/verity/eventstream"
	"github.com/dogmatiq/verity/persistence"
)

// Stream is an implementation of eventstream.Stream that reads events from a
// persistence.EventRepository.
type Stream struct {
	// App is the identity of the application that owns the stream.
	App *identitypb.Identity

	// Types is the set of supported event types.
	Types *sets.Set[message.Type]

	// Repository is the event repository used to load events.
	Repository persistence.EventRepository

	// Cache is an in-memory stream that contains recently recorded events.
	//
	// The cache must be provided, otherwise the stream has no way to block
	// until a new event is recorded.
	Cache eventstream.Stream

	// PreFetch specifies how many messages to pre-load into memory.
	PreFetch int
}

// Application returns the identity of the application that owns the stream.
func (s *Stream) Application() *identitypb.Identity {
	return s.App
}

// EventTypes returns the set of event types that may appear on the stream.
func (s *Stream) EventTypes(context.Context) (*sets.Set[message.Type], error) {
	return s.Types, nil
}

// Open returns a cursor that reads events from the stream.
//
// o is the offset of the first event to read. The first event on a stream is
// always at offset 0.
//
// f is the set of "filter" event types to be returned by Cursor.Next(). Any
// other event types are ignored.
//
// It returns an error if any of the event types in f are not supported, as
// indicated by EventTypes().
func (s *Stream) Open(
	ctx context.Context,
	o uint64,
	f *sets.Set[message.Type],
) (eventstream.Cursor, error) {
	if f.Len() == 0 {
		panic("at least one event type must be specified")
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	rf := map[string]struct{}{}

	for mt := range f.All() {
		n, err := s.Marshaler.MarshalType(mt.ReflectType())
		if err != nil {
			panic(err)
		}

		rf[n] = struct{}{}
	}

	consumeCtx, cancelConsume := context.WithCancel(context.Background())

	c := &cursor{
		repository:       s.Repository,
		repositoryFilter: rf,
		cache:            s.Cache,
		cacheFilter:      f,
		offset:           o,
		cancel:           cancelConsume,
		events:           make(chan eventstream.Event, s.PreFetch),
	}

	go c.consume(consumeCtx)

	return c, nil
}
