package memory

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
)

// Stream is an implementation of persistence.Stream that stores messages
// in-memory.
type Stream struct {
	m         sync.Mutex
	ready     chan struct{}
	envelopes []*envelope.Envelope
}

// Open returns a cursor used to read messages from this stream.
//
// offset is the position of the first message to read. The first message on a
// stream is always at offset 0.
//
// types is a set of message types indicating which message types are returned
// by Cursor.Next().
func (s *Stream) Open(
	ctx context.Context,
	offset uint64,
	types message.TypeCollection,
) (persistence.StreamCursor, error) {
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

// Append appends messages to the stream.
func (s *Stream) Append(envelopes ...*envelope.Envelope) {
	s.m.Lock()
	defer s.m.Unlock()

	s.envelopes = append(s.envelopes, envelopes...)

	if s.ready != nil {
		close(s.ready)
		s.ready = nil
	}
}

// cursor is an implementation of persistence.Cursor that reads messages from an
// in-memory stream.
type cursor struct {
	stream *Stream
	offset uint64
	types  message.TypeCollection
	once   sync.Once
	closed chan struct{}
}

// Next returns the next relevant message in the stream.
//
// If the end of the stream is reached it blocks until a relevant message is
// appended to the stream or ctx is canceled.
func (c *cursor) Next(ctx context.Context) (*persistence.StreamMessage, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, persistence.ErrStreamCursorClosed
		default:
		}

		m, ready := c.get()

		if ready == nil {
			return m, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, persistence.ErrStreamCursorClosed
		case <-ready:
			continue // added to see coverage
		}
	}
}

// Close stops the cursor.
//
// Any current or future calls to Next() return a non-nil error.
func (c *cursor) Close() error {
	err := persistence.ErrStreamCursorClosed

	c.once.Do(func() {
		err = nil
		close(c.closed)
	})

	return err
}

// get returns the next relevant message, or if the end of the stream is
// reached, it returns a "ready" channel that is closed when a message is
// appended.
func (c *cursor) get() (*persistence.StreamMessage, <-chan struct{}) {
	c.stream.m.Lock()
	defer c.stream.m.Unlock()

	for uint64(len(c.stream.envelopes)) > c.offset {
		offset := c.offset
		c.offset++

		env := c.stream.envelopes[offset]

		if c.types.HasM(env.Message) {
			return &persistence.StreamMessage{
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
