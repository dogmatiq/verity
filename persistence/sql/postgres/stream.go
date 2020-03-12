package postgres

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/sql/internal/streamfilter"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit"
)

// Stream is an implementation of persistence.Stream that stores messages
// in a PostgreSQL database.
type Stream struct {
	ApplicationKey  string
	DB              *sql.DB
	Marshaler       marshalkit.Marshaler
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
	filterID, err := s.findOrCreateFilter(ctx, types)
	if err != nil {
		return nil, err
	}

	return &cursor{
		appKey:    s.ApplicationKey,
		offset:    offset,
		filterID:  filterID,
		db:        s.DB,
		marshaler: s.Marshaler,
		counter: backoff.Counter{
			Strategy: s.BackoffStrategy,
		},
		closed: make(chan struct{}),
	}, nil
}

// Append appends messages to the stream.
//
// It returns the next free offset.
func (s *Stream) Append(
	ctx context.Context,
	tx *sql.Tx,
	envelopes ...*envelope.Envelope,
) (_ uint64, err error) {
	defer sqlx.Recover(&err)

	count := uint64(len(envelopes))

	next := sqlx.QueryN(
		ctx,
		tx,
		`INSERT INTO infix.stream_offset AS o (
			next_offset
		) VALUES (
			$1
		) ON CONFLICT (singleton) DO UPDATE SET
			next_offset = o.next_offset + excluded.next_offset
		RETURNING next_offset`,
		count,
	) - count

	for _, env := range envelopes {
		sqlx.Exec(
			ctx,
			tx,
			`INSERT INTO infix.stream (
				stream_offset,
				message_type,
				description,
				message_id,
				causation_id,
				correlation_id,
				source_app_name,
				source_app_key,
				source_handler_name,
				source_handler_key,
				source_instance_id,
				created_at,
				media_type,
				data
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
			)`,
			next,
			marshalkit.MustMarshalType(
				s.Marshaler,
				reflect.TypeOf(env.Message),
			),
			dogma.DescribeMessage(env.Message),
			env.MessageID,
			env.CausationID,
			env.CorrelationID,
			env.Source.Application.Name,
			env.Source.Application.Key,
			env.Source.Handler.Name,
			env.Source.Handler.Key,
			env.Source.InstanceID,
			sqlx.MarshalTime(env.CreatedAt),
			env.Packet.MediaType,
			env.Packet.Data,
		)

		next++
	}

	return next, nil
}

// findOrCreateFilter returns the filter ID for a filter that limits cursor
// results to the given message type names.
//
// If no such filter exists, a new one is created.
func (s *Stream) findOrCreateFilter(
	ctx context.Context,
	types message.TypeCollection,
) (_ uint64, err error) {
	defer sqlx.Recover(&err)

	hash, names := streamfilter.Hash(s.Marshaler, types)

	if id, ok := findFilter(ctx, s.DB, hash, names); ok {
		return id, nil
	}

	return createFilter(ctx, s.DB, hash, names), nil
}

// cursor is an implementation of persistence.Cursor that reads messages from a
// PostgreSQL database.
type cursor struct {
	appKey    string
	offset    uint64
	filterID  uint64
	db        *sql.DB
	marshaler marshalkit.ValueMarshaler
	counter   backoff.Counter
	once      sync.Once
	closed    chan struct{}
}

var errCursorClosed = errors.New("cursor is closed")

// Next returns the next relevant message in the stream.
//
// If the end of the stream is reached it blocks until a relevant message is
// appended to the stream or ctx is canceled.
func (c *cursor) Next(ctx context.Context) (_ *persistence.StreamMessage, err error) {
	// Check immediately if the cursor is already closed.
	select {
	case <-c.closed:
		return nil, errCursorClosed
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
	// was because the cursor was closed, and if so, return a more meaningful.
	defer func() {
		if err == context.Canceled {
			select {
			case <-c.closed:
				err = errCursorClosed
			default:
			}
		}
	}()

	// Convert sqlx panics to errors. This is defer'd LAST so that it recovers
	// FIRST on the way back out. The error value is used in the defered
	// functions above.
	defer sqlx.Recover(&err)

	for {
		row := c.db.QueryRowContext(
			ctx,
			`SELECT
				stream_offset,
				message_id,
				causation_id,
				correlation_id,
				source_app_name,
				source_handler_name,
				source_handler_key,
				source_instance_id,
				created_at,
				media_type,
				data
			FROM infix.stream AS s
			INNER JOIN infix.stream_filter_type AS t
			ON t.message_type = s.message_type
			WHERE s.source_app_key = $1
			AND s.stream_offset >= $2
			AND t.filter_id = $3
			ORDER BY s.stream_offset
			LIMIT 1`,
			c.appKey,
			c.offset,
			c.filterID,
		)

		m := persistence.StreamMessage{
			Envelope: &envelope.Envelope{},
		}

		var createdAt []byte

		if sqlx.TryScan(
			row,
			&m.Offset,
			&m.Envelope.MessageID,
			&m.Envelope.CausationID,
			&m.Envelope.CorrelationID,
			&m.Envelope.Source.Application.Name,
			&m.Envelope.Source.Handler.Name,
			&m.Envelope.Source.Handler.Key,
			&m.Envelope.Source.InstanceID,
			&createdAt,
			&m.Envelope.Packet.MediaType,
			&m.Envelope.Packet.Data,
		) {
			m.Envelope.Source.Application.Key = c.appKey
			m.Envelope.CreatedAt = sqlx.UnmarshalTime(createdAt)
			m.Envelope.Message, err = marshalkit.UnmarshalMessage(
				c.marshaler,
				m.Envelope.Packet,
			)
			if err != nil {
				return nil, err
			}

			c.offset = m.Offset + 1
			c.counter.Reset()

			return &m, nil
		}

		if err := c.counter.Sleep(ctx, nil); err != nil {
			return nil, err
		}
	}
}

// Close stops the cursor.
//
// Any current or future calls to Next() return a non-nil error.
func (c *cursor) Close() (err error) {
	c.once.Do(func() {
		close(c.closed)
	})

	return nil
}
