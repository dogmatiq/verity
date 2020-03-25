package eventstream

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
)

// MemoryStream is an in-memory event stream.
type MemoryStream struct {
	// App is the identity of the application that owns the stream.
	App configkit.Identity

	// Types is the set of supported event types.
	Types message.TypeCollection

	m         sync.Mutex
	ready     chan struct{}
	envelopes []*envelope.Envelope
}

// Application returns the identity of the application that owns the stream.
func (s *MemoryStream) Application() configkit.Identity {
	return s.App
}

// EventTypes returns the set of event types that may appear on the stream.
func (s *MemoryStream) EventTypes(context.Context) (message.TypeCollection, error) {
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
func (s *MemoryStream) Open(
	ctx context.Context,
	o Offset,
	f message.TypeCollection,
) (Cursor, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return &memoryCursor{
		stream: s,
		offset: o,
		closed: make(chan struct{}),
		filter: f,
	}, nil
}

// Append appends events to the stream.
func (s *MemoryStream) Append(envelopes ...*envelope.Envelope) {
	for _, env := range envelopes {
		t := message.TypeOf(env.Message)
		if !s.Types.Has(t) {
			panic("unsupported message type: " + t.String())
		}
	}

	s.m.Lock()
	defer s.m.Unlock()

	s.envelopes = append(s.envelopes, envelopes...)

	if s.ready != nil {
		close(s.ready)
		s.ready = nil
	}
}

// memoryCursor is a Cursor that reads events from a MemoryStream.
type memoryCursor struct {
	stream *MemoryStream
	offset Offset
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
func (c *memoryCursor) Next(ctx context.Context) (*Event, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, ErrCursorClosed
		default:
		}

		ev, ready := c.get()

		if ready == nil {
			return ev, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, ErrCursorClosed
		case <-ready:
			continue // keep to see coverage
		}
	}
}

// Close discards the cursor.
//
// It returns ErrCursorClosed if the cursor is already closed.
// Any current or future calls to Next() return ErrCursorClosed.
func (c *memoryCursor) Close() error {
	err := ErrCursorClosed

	c.once.Do(func() {
		err = nil
		close(c.closed)
	})

	return err
}

// get returns the next relevant event, or if the end of the stream is reached,
// it returns a "ready" channel that is closed when an event is appended.
func (c *memoryCursor) get() (*Event, <-chan struct{}) {
	c.stream.m.Lock()
	defer c.stream.m.Unlock()

	for Offset(len(c.stream.envelopes)) > c.offset {
		offset := c.offset
		c.offset++

		env := c.stream.envelopes[offset]

		if c.filter.HasM(env.Message) {
			return &Event{
				Offset:   offset,
				Envelope: env,
			}, nil
		}
	}

	if c.stream.ready == nil {
		c.stream.ready = make(chan struct{})
	}

	return nil, c.stream.ready
}
