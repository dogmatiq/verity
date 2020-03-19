package memory

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/eventstream"
)

// Stream is an implementation of eventstream.Stream that stores events
// in-memory.
type Stream struct {
	// AppKey is the identity key of the application that owns the stream.
	AppKey string

	// Types is the set of supported event types.
	Types message.TypeCollection

	m         sync.Mutex
	ready     chan struct{}
	envelopes []*envelope.Envelope
}

// ApplicationKey returns the identity key of the application that owns the
// stream.
func (s *Stream) ApplicationKey() string {
	return s.AppKey
}

// Open returns a cursor used to read events from this stream.
//
// offset is the position of the first event to read. The first event on a
// stream is always at offset 0.
//
// types is the set of event types that should be returned by Cursor.Next().
// Any other event types are ignored.
func (s *Stream) Open(
	ctx context.Context,
	offset uint64,
	types message.TypeCollection,
) (eventstream.Cursor, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return &cursor{
		stream: s,
		offset: offset,
		closed: make(chan struct{}),
		types:  types,
	}, nil
}

// MessageTypes returns the complete set of event types that may appear on the
// stream.
func (s *Stream) MessageTypes(context.Context) (message.TypeCollection, error) {
	return s.Types, nil
}

// Append appends events to the stream.
func (s *Stream) Append(envelopes ...*envelope.Envelope) {
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

// cursor is an implementation of eventstream.Cursor that reads events from an
// in-memory stream.
type cursor struct {
	stream *Stream
	offset uint64
	types  message.TypeCollection

	once   sync.Once
	closed chan struct{}
}

// Next returns the next relevant event in the stream.
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

		ev, ready := c.get()

		if ready == nil {
			return ev, nil
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

// Close stops the cursor.
//
// It returns ErrCursorClosed if the cursor is already closed.
//
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

	for uint64(len(c.stream.envelopes)) > c.offset {
		offset := c.offset
		c.offset++

		env := c.stream.envelopes[offset]

		if c.types.HasM(env.Message) {
			return &eventstream.Event{
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
