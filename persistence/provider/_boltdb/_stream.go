package boltdb

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/marshalkit"
	"go.etcd.io/bbolt"
)

var (
	eventStreamKey = []byte("eventstream")
	offsetKey      = []byte("offset")
	eventsKey      = []byte("events")
)

// Stream is an implementation of eventstream.Stream that stores events in a
// BoltDB database.
type Stream struct {
	// App is the identity of the application that owns the stream.
	App configkit.Identity

	// DB is the BoltDB database containing the stream's data.
	DB *bbolt.DB

	// Types is the set of supported event types.
	Types message.TypeCollection

	// Marshaler is used to marshal and unmarshal events for storage.
	Marshaler marshalkit.ValueMarshaler

	m     sync.Mutex
	ready chan struct{}
}

// Application returns the identity of the application that owns the stream.
func (s *Stream) Application() configkit.Identity {
	return s.App
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
	offset eventstream.Offset,
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
//
// It returns the next free offset.
func (s *Stream) Append(
	tx *bbolt.Tx,
	envelopes ...*envelope.Envelope,
) (_ eventstream.Offset, err error) {
	defer bboltx.Recover(&err)

	b := bboltx.CreateBucketIfNotExists(
		tx,
		[]byte(s.App.Key),
		eventStreamKey,
	)

	next := loadNextOffset(b)
	next = appendEvents(b, next, s.Types, envelopes)
	storeNextOffset(b, next)

	tx.OnCommit(func() {
		s.m.Lock()
		defer s.m.Unlock()

		if s.ready != nil {
			close(s.ready)
			s.ready = nil
		}
	})

	return next, nil
}

// cursor is an implementation of eventstream.Cursor that reads events from a
// BoltDB database.
type cursor struct {
	stream *Stream
	offset eventstream.Offset
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
func (c *cursor) Next(ctx context.Context) (_ *eventstream.Event, err error) {
	defer bboltx.Recover(&err)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, eventstream.ErrCursorClosed
		default:
		}

		m, ready := c.get()

		if ready == nil {
			return m, err
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
	tx := bboltx.BeginRead(c.stream.DB)
	defer tx.Rollback()

	if b, ok := bboltx.TryBucket(
		tx,
		[]byte(c.stream.App.Key),
		eventStreamKey,
	); ok {
		next := loadNextOffset(b)

		for next > c.offset {
			offset := c.offset
			env := loadMessage(c.stream.Marshaler, b, offset)

			c.offset++

			if c.types.HasM(env.Message) {
				return &eventstream.Event{
					Offset:   offset,
					Envelope: env,
				}, nil
			}
		}
	}

	c.stream.m.Lock()
	defer c.stream.m.Unlock()

	if c.stream.ready == nil {
		c.stream.ready = make(chan struct{})
	}

	return nil, c.stream.ready
}
