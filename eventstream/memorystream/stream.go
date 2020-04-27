package memorystream

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/parcel"
)

// Stream is an in-memory event stream.
type Stream struct {
	// App is the identity of the application that owns the stream.
	App configkit.Identity

	// Types is the set of supported event types.
	Types message.TypeCollection

	m      sync.Mutex
	ready  chan struct{}
	events []*eventstream.Event
}

// Application returns the identity of the application that owns the stream.
func (s *Stream) Application() configkit.Identity {
	return s.App
}

// EventTypes returns the set of event types that may appear on the stream.
func (s *Stream) EventTypes(context.Context) (message.TypeCollection, error) {
	return s.Types, nil
}

// Open returns a cursor that reads events from the stream.
//
// o is the offset of the first event to read. The first event on a stream
// is always at offset 0.
//
// f is the set of "filter" event types to be returned by Cursor.Next(). Any
// other event types are ignored.
//
// It returns an error if any of the event types in f are not supported, as
// indicated by EventTypes().
func (s *Stream) Open(
	ctx context.Context,
	o uint64,
	f message.TypeCollection,
) (eventstream.Cursor, error) {
	if f.Len() == 0 {
		panic("at least one event type must be specified")
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return &cursor{
		stream: s,
		offset: o,
		closed: make(chan struct{}),
		filter: f,
	}, nil
}

// Append appends events to the stream.
func (s *Stream) Append(parcels ...*parcel.Parcel) {
	s.m.Lock()
	defer s.m.Unlock()

	o := uint64(len(s.events))

	for _, p := range parcels {
		t := message.TypeOf(p.Message)
		if !s.Types.Has(t) {
			panic("unsupported message type: " + t.String())
		}

		s.events = append(
			s.events,
			&eventstream.Event{
				Offset: o,
				Parcel: p,
			},
		)

		o++
	}

	if s.ready != nil {
		close(s.ready)
		s.ready = nil
	}
}
