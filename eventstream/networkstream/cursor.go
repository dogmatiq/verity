package networkstream

import (
	"context"
	"sync"

	"github.com/dogmatiq/enginekit/marshaler"
	"github.com/dogmatiq/interopspec/eventstreamspec"
	"github.com/dogmatiq/verity/eventstream"
	"github.com/dogmatiq/verity/parcel"
)

// cursor is a Cursor that reads events from a network stream.
type cursor struct {
	stream    eventstreamspec.StreamAPI_ConsumeClient
	marshaler marshaler.Marshaler
	once      sync.Once
	cancel    context.CancelFunc
	events    chan eventstream.Event
	err       error
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
	case ev, ok := <-c.events:
		if ok {
			return ev, nil
		}

		return eventstream.Event{}, c.err
	}
}

// Close stops the cursor.
//
// It returns ErrCursorClosed if the cursor is already closed.
//
// Any current or future calls to Next() return ErrCursorClosed.
func (c *cursor) Close() error {
	if !c.close(eventstream.ErrCursorClosed) {
		return eventstream.ErrCursorClosed
	}

	return nil
}

// consume receives new events, unmarshals them, and pipes them over the
// c.events channel to a goroutine that calls Next().
//
// It exits when the context associated with c.stream is canceled or some other
// error occurs while reading from the stream.
func (c *cursor) consume() {
	defer close(c.events)

	for {
		err := c.recv()

		if err != nil {
			c.close(err)
			return
		}
	}
}

// close closes the cursor. It returns false if the cursor was already closed.
func (c *cursor) close(cause error) bool {
	ok := false

	c.once.Do(func() {
		c.cancel()
		c.err = cause
		ok = true
	})

	return ok
}

// recv waits for the next event from the stream, unmarshals it and sends it
// over the c.events channel.
func (c *cursor) recv() error {
	// We can't pass ctx to Recv(), but the stream is already bound to a context.
	res, err := c.stream.Recv()
	if err != nil {
		return err
	}

	env := res.GetEnvelope()

	ev := eventstream.Event{
		Offset: res.Offset,
	}

	ev.Parcel, err = parcel.FromEnvelope(c.marshaler, env)
	if err != nil {
		return err
	}

	select {
	case c.events <- ev:
		return nil
	case <-c.stream.Context().Done():
		return c.stream.Context().Err()
	}
}
