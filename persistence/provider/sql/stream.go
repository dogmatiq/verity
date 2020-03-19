package sql

import (
	"context"
	"database/sql"
	"reflect"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver"
	"github.com/dogmatiq/infix/persistence/provider/sql/internal/streamfilter"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit"
)

// Stream is an implementation of eventstream.Stream that stores events in an
// SQL database.
type Stream struct {
	// AppKey is the identity key of the application that owns the stream.
	AppKey string

	// DB is the SQL database containing the stream's data.
	DB *sql.DB

	// Driver performs the database-system-specific SQL queries.
	Driver driver.StreamDriver

	// Types is the set of supported event types.
	Types message.TypeCollection

	// Marshaler is used to marshal and unmarshal events for storage.
	Marshaler marshalkit.Marshaler

	// BackoffStrategy is the backoff strategy used to determine delays betweens
	// polls that do not produce any results.
	BackoffStrategy backoff.Strategy
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
	hash, names := streamfilter.Hash(s.Marshaler, types)

	filterID, ok, err := s.Driver.FindFilter(ctx, s.DB, hash, names)
	if err != nil {
		return nil, err
	}

	if !ok {
		filterID, err = s.Driver.CreateFilter(ctx, s.DB, hash, names)
		if err != nil {
			return nil, err
		}
	}

	strategy := s.BackoffStrategy
	if strategy == nil {
		strategy = DefaultStreamBackoff
	}

	return &cursor{
		stream:   s,
		offset:   offset,
		filterID: filterID,
		counter: backoff.Counter{
			Strategy: strategy,
		},
		closed: make(chan struct{}),
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
	ctx context.Context,
	tx *sql.Tx,
	envelopes ...*envelope.Envelope,
) (uint64, error) {
	count := uint64(len(envelopes))

	next, err := s.Driver.IncrementOffset(
		ctx,
		tx,
		s.AppKey,
		count,
	)
	if err != nil {
		return 0, err
	}

	next -= count

	for _, env := range envelopes {
		if !s.Types.HasM(env.Message) {
			panic("unsupported message type: " + message.TypeOf(env.Message).String())
		}

		if err := s.Driver.Append(
			ctx,
			tx,
			next,
			marshalkit.MustMarshalType(
				s.Marshaler,
				reflect.TypeOf(env.Message),
			),
			dogma.DescribeMessage(env.Message),
			env,
		); err != nil {
			return 0, err
		}

		next++
	}

	return next, nil
}

// cursor is an implementation of eventstream.Cursor that reads messages from an
// SQL database.
type cursor struct {
	stream   *Stream
	offset   uint64
	filterID uint64
	counter  backoff.Counter

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
	// Check immediately if the cursor is already closed.
	select {
	case <-c.closed:
		return nil, eventstream.ErrCursorClosed
	default:
	}

	// Otherwise, setup a context that we can cancel when the cursor is closed.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-ctx.Done():
		case <-c.closed:
			cancel()
		}
	}()

	// Finally, if we actually get a context cancelleation error, check if it
	// was because the cursor was closed, and if so, return a more meaningful
	// error.
	defer func() {
		if err == context.Canceled {
			select {
			case <-c.closed:
				err = eventstream.ErrCursorClosed
			default:
			}
		}
	}()

	for {
		ev, ok, err := c.stream.Driver.Get(
			ctx,
			c.stream.DB,
			c.stream.AppKey,
			c.offset,
			c.filterID,
		)
		if err != nil {
			return nil, err
		}

		if ok {
			ev.Envelope.Source.Application.Key = c.stream.AppKey

			if ev.Envelope.Message == nil {
				ev.Envelope.Message, err = marshalkit.UnmarshalMessage(
					c.stream.Marshaler,
					ev.Envelope.Packet,
				)
				if err != nil {
					return nil, err
				}
			}

			c.offset = ev.Offset + 1
			c.counter.Reset()

			return ev, nil
		}

		if err := c.counter.Sleep(ctx, nil); err != nil {
			return nil, err
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
