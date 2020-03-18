package boltdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	"go.etcd.io/bbolt"
)

var (
	offsetKey   = []byte("offset")
	messagesKey = []byte("messages")
)

// Stream is an implementation of persistence.Stream that stores messages
// in a BoltDB database.
type Stream struct {
	// DB is the BoltDB database containing the stream's data.
	DB *bbolt.DB

	// Types is the set of supported message types.
	Types message.TypeCollection

	// Marshaler is used to marshal and unmarshal messages for storage.
	Marshaler marshalkit.ValueMarshaler

	// BucketPath is the "path" to a nested bucket with the database that
	// contains the stream's data.
	BucketPath [][]byte

	m     sync.Mutex
	ready chan struct{}
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
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return &cursor{
		stream: s,
		offset: offset,
		closed: make(chan struct{}),
		types:  types,
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
	tx *bbolt.Tx,
	envelopes ...*envelope.Envelope,
) (_ uint64, err error) {
	defer bboltx.Recover(&err)

	b := bboltx.CreateBucketIfNotExists(tx, s.BucketPath...)
	next := loadNextOffset(b)
	next = appendMessages(b, next, s.Types, envelopes)
	storeNextOffset(b, next)

	tx.OnCommit(func() {
		s.m.Lock()
		defer s.m.Unlock()

		if s.ready != nil {
			close(s.ready)
			s.ready = nil
		}
	})

	return next, nil
}

// cursor is an implementation of persistence.Cursor that reads messages from a
// BoltDB database.
type cursor struct {
	stream *Stream
	offset uint64
	types  message.TypeCollection

	once   sync.Once
	closed chan struct{}
}

// Next returns the next relevant message in the stream.
//
// If the end of the stream is reached it blocks until a relevant message is
// appended to the stream or ctx is canceled.
func (c *cursor) Next(ctx context.Context) (_ *persistence.StreamMessage, err error) {
	defer bboltx.Recover(&err)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, persistence.ErrStreamCursorClosed
		default:
		}

		m, ready := c.get()

		if ready == nil {
			return m, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, persistence.ErrStreamCursorClosed
		case <-ready:
			continue // keep to see coverage
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

// get returns the next relevant message, or if the end of the stream is
// reached, it returns a "ready" channel that is closed when a message is
// appended.
func (c *cursor) get() (*persistence.StreamMessage, <-chan struct{}) {
	tx := bboltx.BeginRead(c.stream.DB)
	defer tx.Rollback()

	if b := bboltx.Bucket(tx, c.stream.BucketPath...); b != nil {
		next := loadNextOffset(b)

		for next > c.offset {
			offset := c.offset
			env := loadMessage(c.stream.Marshaler, b, offset)

			c.offset++

			if c.types.HasM(env.Message) {
				return &persistence.StreamMessage{
					Offset:   offset,
					Envelope: env,
				}, nil
			}
		}
	}

	c.stream.m.Lock()
	defer c.stream.m.Unlock()

	if c.stream.ready == nil {
		c.stream.ready = make(chan struct{})
	}

	return nil, c.stream.ready
}

// marshalOffset marshals a stream offset to its binary representation.
func marshalOffset(offset uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, offset)
	return data
}

// unmarshalOffset unmarshals a stream offset from its binary representation.
func unmarshalOffset(data []byte) uint64 {
	n := len(data)

	switch n {
	case 0:
		return 0
	case 8:
		return binary.BigEndian.Uint64(data)
	default:
		panic(bboltx.PanicSentinel{
			Cause: fmt.Errorf("offset data is corrupt, expected 8 bytes, got %d", n),
		})
	}
}

// loadNextOffset returns the next free offset.
func loadNextOffset(b *bbolt.Bucket) uint64 {
	data := b.Get(offsetKey)
	return unmarshalOffset(data)
}

// storeNextOffset updates the next free offset.
func storeNextOffset(b *bbolt.Bucket, next uint64) {
	data := marshalOffset(next)
	bboltx.Put(b, offsetKey, data)
}

// loadMessage loads a message at a specific offset.
func loadMessage(
	m marshalkit.ValueMarshaler,
	b *bbolt.Bucket,
	offset uint64,
) *envelope.Envelope {
	k := marshalOffset(offset)
	v := b.Bucket(messagesKey).Get(k)

	var env envelope.Envelope
	bboltx.Must(envelope.UnmarshalBinary(m, v, &env))

	return &env
}

// appendMessages writes messages to the database.
func appendMessages(
	b *bbolt.Bucket,
	next uint64,
	types message.TypeCollection,
	envelopes []*envelope.Envelope,
) uint64 {
	messages := bboltx.CreateBucketIfNotExists(b, messagesKey)

	for _, env := range envelopes {
		if !types.HasM(env.Message) {
			panic("unsupported message type: " + message.TypeOf(env.Message).String())
		}

		k := marshalOffset(next)
		v := envelope.MustMarshalBinary(env)
		bboltx.Put(messages, k, v)
		next++
	}

	return next
}
