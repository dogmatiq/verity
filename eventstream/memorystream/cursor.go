package memorystream

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
)

// cursor is a Cursor that reads events from an in-memory stream.
type cursor struct {
	stream *Stream
	offset uint64
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

	for uint64(len(c.stream.events)) > c.offset {
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
