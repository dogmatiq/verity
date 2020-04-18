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
	o eventstream.Offset,
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

	o := eventstream.Offset(len(s.events))

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

// cursor is a Cursor that reads events from a MemoryStream.
type cursor struct {
	stream *Stream
	offset eventstream.Offset
	filter message.TypeCollection

	once   sync.Once
	closed chan struct{}
}

// Next returns the next event in the stream that matches the filter.
//
// If the end of the stream is reached it blocks until a relevant event is
// appended to the stream or ctx is canceled.
//
// If the stream is closed before or during a call to Next(), it returns
// ErrCursorClosed.
func (c *cursor) Next(ctx context.Context) (*eventstream.Event, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, eventstream.ErrCursorClosed
		default:
		}

		env, ready := c.get()

		if ready == nil {
			return env, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, eventstream.ErrCursorClosed
		case <-ready:
			continue // keep to see coverage
		}
	}
}

// Close discards the cursor.
//
// It returns ErrCursorClosed if the cursor is already closed.
// Any current or future calls to Next() return ErrCursorClosed.
func (c *cursor) Close() error {
	err := eventstream.ErrCursorClosed

	c.once.Do(func() {
		err = nil
		close(c.closed)
	})

	return err
}

// get returns the next relevant event, or if the end of the stream is reached,
// it returns a "ready" channel that is closed when an event is appended.
func (c *cursor) get() (*eventstream.Event, <-chan struct{}) {
	c.stream.m.Lock()
	defer c.stream.m.Unlock()

	for eventstream.Offset(len(c.stream.events)) > c.offset {
		offset := c.offset
		c.offset++

		ev := c.stream.events[offset]

		if c.filter.HasM(ev.Parcel.Message) {
			return ev, nil
		}
	}

	if c.stream.ready == nil {
		c.stream.ready = make(chan struct{})
	}

	return nil, c.stream.ready
}
