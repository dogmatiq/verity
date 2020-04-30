package persistedstream

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/marshalkit"
)

// cursor is an eventstream.Cursor that reads events from the event store.
type cursor struct {
	repository eventstore.Repository
	query      eventstore.Query
	marshaler  marshalkit.ValueMarshaler
	cache      eventstream.Stream
	filter     message.TypeCollection
	once       sync.Once
	cancel     context.CancelFunc
	events     chan *eventstream.Event
	err        error
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
	case ev, ok := <-c.events:
		if ok {
			return ev, nil
		}

		return nil, c.err
	}
}

// Close discards the cursor.
//
// It returns ErrCursorClosed if the cursor is already closed.
// Any current or future calls to Next() return ErrCursorClosed.
func (c *cursor) Close() error {
	if !c.close(eventstream.ErrCursorClosed) {
		return eventstream.ErrCursorClosed
	}

	return nil
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

// consume queries the store, unmarshals the query results, and pipes the events
// over the c.events channel to a goroutine that calls Next().
//
// It exits when the ctx is canceled or some other error occurs while reading
// from the underlying cursor.
func (c *cursor) consume(ctx context.Context) {
	defer close(c.events)

	for {
		if err := c.consumeFromCache(ctx); err != nil {
			c.close(err)
			return
		}

		if err := c.consumeFromStore(ctx); err != nil {
			c.close(err)
			return
		}
	}
}

// consumeFromCache attempts to obtain events from the in-memory cache.
//
// A nil return value indicates that the cache does not have the required events
// and that the event store should be queried instead.
func (c *cursor) consumeFromCache(ctx context.Context) error {
	cur, err := c.cache.Open(ctx, c.query.MinOffset, c.filter)
	if err != nil {
		return err
	}
	defer cur.Close()

	for {
		ev, err := cur.Next(ctx)
		if err == eventstream.ErrTruncated {
			return nil
		} else if err != nil {
			return err
		}

		if err := c.send(ctx, ev); err != nil {
			return err
		}
	}
}

// consumeFromStore attempts to obtain events from the event store.
//
// A nil return value indicates that the requested offset is not in the store so
// that the cache (which blocks until new events are recorded) should be used
// instead.
func (c *cursor) consumeFromStore(ctx context.Context) error {
	// Query the "next" offset before we run our event query. When we exhaust
	// the result of QueryEvents() we know we've at least checked up to this
	// offset, even if the event AT this offset doesn't match our filter.
	next, err := c.repository.NextEventOffset(ctx)
	if err != nil {
		return err
	}

	res, err := c.repository.QueryEvents(ctx, c.query)
	if err != nil {
		return err
	}
	defer res.Close()

	for {
		i, ok, err := res.Next(ctx)
		if err != nil {
			return err
		}

		if !ok {
			// We've run out of events from the store, so we bail to consume
			// from the recent event cache instead.

			if next > c.query.MinOffset {
				// There were more events after the last event that matched our
				// filter, we skip over all of them when we hit the cache.
				c.query.MinOffset = next
			}

			return nil
		}

		ev := &eventstream.Event{
			Offset: i.Offset,
		}

		ev.Parcel, err = parcel.FromEnvelope(c.marshaler, i.Envelope)
		if err != nil {
			return err
		}

		if err := c.send(ctx, ev); err != nil {
			return err
		}
	}
}

// send writes ev to c.events.
func (c *cursor) send(ctx context.Context, ev *eventstream.Event) error {
	select {
	case c.events <- ev:
		c.query.MinOffset = ev.Offset + 1
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
