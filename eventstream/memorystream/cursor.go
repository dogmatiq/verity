package memorystream

import (
	"context"
	"sync"

	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/verity/eventstream"
)

// cursor is a Cursor that reads events from an in-memory stream.
type cursor struct {
	offset uint64
	filter *sets.Set[message.Type]
	node   *node

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
//
// It returns ErrTruncated if the next event can not be obtained because it
// occupies a portion of the stream that has been truncated.
func (c *cursor) Next(ctx context.Context) (eventstream.Event, error) {
	select {
	case <-ctx.Done():
		return eventstream.Event{}, ctx.Err()
	case <-c.closed:
		return eventstream.Event{}, eventstream.ErrCursorClosed
	default:
	}

	// The offset we want is before the node we have, we must have requested an
	// offset that has already been truncated from the buffer.
	if c.offset < c.node.offset {
		return eventstream.Event{}, eventstream.ErrTruncated
	}

	for {
		ev, err := c.wait(ctx)
		if err != nil {
			return eventstream.Event{}, err
		}

		if ev.Offset == c.offset {
			c.offset++

			if c.filter.Has(message.TypeOf(ev.Parcel.Message)) {
				return ev, nil
			}
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

// wait blocks until the event in c.node is available, then returns it.
// c.node advances to the next node on success.
func (c *cursor) wait(ctx context.Context) (eventstream.Event, error) {
	if ch := c.node.ready(); ch != nil {
		select {
		case <-ctx.Done():
			return eventstream.Event{}, ctx.Err()
		case <-c.closed:
			return eventstream.Event{}, eventstream.ErrCursorClosed
		case <-ch:
			break // keep to see coverage
		}
	}

	ev := c.node.event
	c.node = c.node.next

	return ev, nil
}
