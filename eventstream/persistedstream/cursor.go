package persistedstream

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/marshaler"
	"github.com/dogmatiq/verity/eventstream"
	"github.com/dogmatiq/verity/parcel"
	"github.com/dogmatiq/verity/persistence"
)

// cursor is an eventstream.Cursor that reads events from an event repository.
type cursor struct {
	repository       persistence.EventRepository
	repositoryFilter map[string]struct{}
	marshaler        marshaler.Marshaler
	cache            eventstream.Stream
	cacheFilter      *sets.Set[message.Type]
	offset           uint64
	once             sync.Once
	cancel           context.CancelFunc
	events           chan eventstream.Event
	err              error
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

// consume obtains events from the cache or the event repository as necessary,
// and pipes the them over the c.events channel to a goroutine that calls
// Next().
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

		if err := c.consumeFromRepository(ctx); err != nil {
			c.close(err)
			return
		}
	}
}

// consumeFromCache attempts to obtain events from the in-memory cache.
//
// A nil return value indicates that the cache does not have the required events
// and that the repository should be queried instead.
func (c *cursor) consumeFromCache(ctx context.Context) error {
	cur, err := c.cache.Open(ctx, c.offset, c.cacheFilter)
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

// consumeFromRepository attempts to obtain events from the event repository.
//
// A nil return value indicates that the requested offset is not in the
// repository and that the cache (which blocks until new events are recorded)
// should be used instead.
func (c *cursor) consumeFromRepository(ctx context.Context) error {
	// Query the "next" offset before we run our event query. When we exhaust
	// the result of QueryEvents() we know we've at least checked up to this
	// offset, even if the event AT this offset doesn't match our filter.
	next, err := c.repository.NextEventOffset(ctx)
	if err != nil {
		return err
	}

	res, err := c.repository.LoadEventsByType(ctx, c.repositoryFilter, c.offset)
	if err != nil {
		return err
	}
	defer res.Close()

	for {
		pev, ok, err := res.Next(ctx)
		if err != nil {
			return err
		}

		if !ok {
			// We've run out of events from the repository, so we bail to
			// consume from the recent event cache instead.

			if next > c.offset {
				// There were more events after the last event that matched our
				// filter, we skip over all of them when we hit the cache.
				c.offset = next
			}

			return nil
		}

		ev := eventstream.Event{
			Offset: pev.Offset,
		}

		ev.Parcel, err = parcel.FromEnvelope(c.marshaler, pev.Envelope)
		if err != nil {
			return err
		}

		if err := c.send(ctx, ev); err != nil {
			return err
		}
	}
}

// send writes ev to c.events.
func (c *cursor) send(ctx context.Context, ev eventstream.Event) error {
	select {
	case c.events <- ev:
		c.offset = ev.Offset + 1
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
