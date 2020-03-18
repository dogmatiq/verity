package sql

import (
	"context"
	"database/sql"
	"reflect"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver"
	"github.com/dogmatiq/infix/persistence/provider/sql/internal/streamfilter"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit"
)

// Stream is an implementation of persistence.Stream that stores messages
// in an SQL database.
type Stream struct {
	// ApplicationKey is the identity key of the source application.
	ApplicationKey string

	// DB is the SQL database containing the stream's data.
	DB *sql.DB

	// Driver performs the database-system-specific SQL queries.
	Driver driver.StreamDriver

	// Types is the set of supported message types.
	Types message.TypeCollection

	// Marshaler is used to marshal and unmarshal messages for storage.
	Marshaler marshalkit.Marshaler

	// BackoffStrategy is the backoff strategy used to determine delays betweens
	// polls that do not produce any results.
	BackoffStrategy backoff.Strategy
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

// MessageTypes returns the message types that may appear on the stream.
func (s *Stream) MessageTypes(context.Context) (message.TypeCollection, error) {
	return s.Types, nil
}

// Append appends messages to the stream.
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
		s.ApplicationKey,
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

// cursor is an implementation of persistence.Cursor that reads messages from an
// SQL database.
type cursor struct {
	stream   *Stream
	offset   uint64
	filterID uint64
	counter  backoff.Counter

	once   sync.Once
	closed chan struct{}
}

// Next returns the next relevant message in the stream.
//
// If the end of the stream is reached it blocks until a relevant message is
// appended to the stream or ctx is canceled.
func (c *cursor) Next(ctx context.Context) (_ *persistence.StreamMessage, err error) {
	// Check immediately if the cursor is already closed.
	select {
	case <-c.closed:
		return nil, persistence.ErrStreamCursorClosed
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
				err = persistence.ErrStreamCursorClosed
			default:
			}
		}
	}()

	for {
		m, ok, err := c.stream.Driver.Get(
			ctx,
			c.stream.DB,
			c.stream.ApplicationKey,
			c.offset,
			c.filterID,
		)
		if err != nil {
			return nil, err
		}

		if ok {
			m.Envelope.Source.Application.Key = c.stream.ApplicationKey

			if m.Envelope.Message == nil {
				m.Envelope.Message, err = marshalkit.UnmarshalMessage(
					c.stream.Marshaler,
					m.Envelope.Packet,
				)
				if err != nil {
					return nil, err
				}
			}

			c.offset = m.Offset + 1
			c.counter.Reset()

			return m, nil
		}

		if err := c.counter.Sleep(ctx, nil); err != nil {
			return nil, err
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
