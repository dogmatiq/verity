package eventstream

import (
	"context"
	"sync"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence/eventstore"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/marshalkit"
)

// EventStoreStream is an implementation of Stream that reads events from a
// eventstore.Repository.
type EventStoreStream struct {
	// App is the identity of the application that owns the stream.
	App configkit.Identity

	// Types is the set of supported event types.
	Types message.TypeCollection

	// Repository is the event store repository used to query events.
	Repository eventstore.Repository

	// Marshaler is used to unmarshal messages.
	Marshaler marshalkit.Marshaler

	// PreFetch specifies how many messages to pre-load into memory.
	PreFetch int
}

// Application returns the identity of the application that owns the stream.
func (s *EventStoreStream) Application() configkit.Identity {
	return s.App
}

// EventTypes returns the set of event types that may appear on the stream.
func (s *EventStoreStream) EventTypes(context.Context) (message.TypeCollection, error) {
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
func (s *EventStoreStream) Open(
	ctx context.Context,
	o Offset,
	f message.TypeCollection,
) (Cursor, error) {
	if f.Len() == 0 {
		panic("at least one event type must be specified")
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	q := eventstore.Query{
		MinOffset: eventstore.Offset(o),
	}

	f.Range(func(mt message.Type) bool {
		n := marshalkit.MustMarshalType(
			s.Marshaler,
			mt.ReflectType(),
		)

		q.Filter.Add(n)

		return true
	})

	consumeCtx, cancelConsume := context.WithCancel(context.Background())

	c := &eventStoreCursor{
		repository: s.Repository,
		query:      q,
		marshaler:  s.Marshaler,
		cancel:     cancelConsume,
		events:     make(chan *Event, s.PreFetch),
	}

	go c.consume(consumeCtx)

	return c, nil
}

// eventStoreCursor is a Cursor that reads events from an EventStoreStream.
type eventStoreCursor struct {
	repository eventstore.Repository
	query      eventstore.Query
	marshaler  marshalkit.ValueMarshaler
	once       sync.Once
	cancel     context.CancelFunc
	events     chan *Event
	err        error
}

// Next returns the next event in the stream that matches the filter.
//
// If the end of the stream is reached it blocks until a relevant event is
// appended to the stream or ctx is canceled.
//
// If the stream is closed before or during a call to Next(), it returns
// ErrCursorClosed.
func (c *eventStoreCursor) Next(ctx context.Context) (*Event, error) {
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
func (c *eventStoreCursor) Close() error {
	if !c.close(ErrCursorClosed) {
		return ErrCursorClosed
	}

	return nil
}

// close closes the cursor. It returns false if the cursor was already closed.
func (c *eventStoreCursor) close(cause error) bool {
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
func (c *eventStoreCursor) consume(ctx context.Context) {
	defer close(c.events)

	for {
		err := c.execQuery(ctx)

		if err != nil {
			c.close(err)
			return
		}
	}
}

// execQuery executes a query against the event store to obtain the next batch
// of events.
func (c *eventStoreCursor) execQuery(ctx context.Context) error {
	res, err := c.repository.QueryEvents(ctx, c.query)
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
			break
		}

		ev := &Event{
			Offset:   Offset(pev.Offset),
			Envelope: &envelope.Envelope{},
		}

		if err := envelope.Unmarshal(
			c.marshaler,
			pev.Envelope,
			ev.Envelope,
		); err != nil {
			return err
		}

		ev.Envelope.Message, err = marshalkit.UnmarshalMessage(
			c.marshaler,
			ev.Envelope.Packet,
		)
		if err != nil {
			return err
		}

		select {
		case c.events <- ev:
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// TODO: https://github.com/dogmatiq/infix/issues/74
	//
	// use a signaling channel to wake the consumer when an event is saved to
	// the store.
	return linger.Sleep(ctx, 100*time.Millisecond)
}
