package memorystream

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
)

// cursor is a Cursor that reads events from an in-memory stream.
type cursor struct {
	offset uint64
	filter message.TypeCollection
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
func (c *cursor) Next(ctx context.Context) (*eventstream.Event, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, eventstream.ErrCursorClosed
	default:
	}

	for {
		// Advance the head node until we reach the one that contains c.offset.
		for c.offset >= c.node.end {
			head, err := c.node.advance(ctx, c.closed)
			if err != nil {
				return nil, err
			}

			c.node = head
		}

		index := int(c.offset - c.node.begin)

		// Iterate the parcels in the node to find one that matches the filter.
		for _, p := range c.node.parcels[index:] {
			o := c.offset
			c.offset++

			if c.filter.HasM(p.Message) {
				return &eventstream.Event{
					Offset: o,
					Parcel: p,
				}, nil
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
