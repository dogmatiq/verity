package boltdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	"go.etcd.io/bbolt"
)

var (
	offsetKey   = []byte("offset")
	messagesKey = []byte("messages")
)

// Stream is an implementation of persistence.Stream that stores messages
// a BoltDB database.
type Stream struct {
	DB         *bbolt.DB
	Marshaler  marshalkit.ValueMarshaler
	BucketPath [][]byte

	rm    sync.RWMutex
	wm    sync.Mutex
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

// Append appends messages to the stream.
//
// It returns the next free offset.
func (s *Stream) Append(
	tx *bbolt.Tx,
	envelopes ...*envelope.Envelope,
) (uint64, error) {
	if tx.DB() != s.DB {
		panic("transaction does not belong to the correct database")
	}

	b, err := createBucket(tx, s.BucketPath...)
	if err != nil {
		return 0, err
	}

	next, err := loadNextOffset(b)
	if err != nil {
		return 0, err
	}

	if len(envelopes) == 0 {
		return next, nil
	}

	next, err = appendMessages(b, next, envelopes)
	if err != nil {
		return 0, err
	}

	if err := storeNextOffset(b, next); err != nil {
		return 0, err
	}

	tx.OnCommit(func() {
		s.rm.Lock()
		defer s.rm.Unlock()

		s.wm.Lock()
		defer s.wm.Unlock()

		if s.ready != nil {
			close(s.ready)
			s.ready = nil
		}
	})

	return next, nil
}

type cursor struct {
	stream *Stream
	offset uint64
	types  message.TypeCollection
	closed chan struct{}
}

var errCursorClosed = errors.New("cursor is closed")

// Next returns the next relevant message in the stream.
//
// If the end of the stream is reached it blocks until a relevant message is
// appended to the stream or ctx is canceled.
func (c *cursor) Next(ctx context.Context) (*persistence.StreamMessage, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, errCursorClosed
		default:
		}

		m, ready, err := c.get()

		if err != nil || ready == nil {
			return m, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, errCursorClosed
		case <-ready:
			continue // added to see coverage
		}
	}
}

// Close stops the cursor.
//
// Any current or future calls to Next() return a non-nil error.
func (c *cursor) Close() error {
	defer func() {
		recover()
	}()

	close(c.closed)

	return nil
}

// get returns the next relevant message, or if the end of the stream is
// reached, it returns a "ready" channel that is closed when a message is
// appended.
func (c *cursor) get() (*persistence.StreamMessage, <-chan struct{}, error) {
	c.stream.rm.RLock()
	defer c.stream.rm.RUnlock()

	var m *persistence.StreamMessage

	err := c.stream.DB.View(func(tx *bbolt.Tx) error {
		var next uint64

		if b, ok := getBucket(tx, c.stream.BucketPath...); ok {
			var err error
			next, err = loadNextOffset(b)
			if err != nil {
				return err
			}

			for next > c.offset {
				offset := c.offset

				env, err := loadMessage(c.stream.Marshaler, b, offset)
				if err != nil {
					return err
				}

				c.offset++

				if c.types.HasM(env.Message) {
					m = &persistence.StreamMessage{
						Offset:   offset,
						Envelope: env,
					}

					return nil
				}
			}
		}

		return nil
	})

	if m != nil || err != nil {
		return m, nil, err
	}

	if c.stream.ready == nil {
		c.stream.wm.Lock()
		defer c.stream.wm.Unlock()
		c.stream.ready = make(chan struct{})
	}

	return nil, c.stream.ready, nil
}

// marshalOffset marshals a stream offset to its binary representation.
func marshalOffset(offset uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, offset)
	return data
}

// unmarshalOffset unmarshals a stream offset from its binary representation.
func unmarshalOffset(data []byte) (uint64, error) {
	n := len(data)

	switch n {
	case 0:
		return 0, nil
	case 8:
		return binary.BigEndian.Uint64(data), nil
	default:
		return 0, fmt.Errorf("offset data is corrupt, expected 8 bytes, got %d", n)
	}
}

// loadNextOffset returns the next free offset.
func loadNextOffset(b *bbolt.Bucket) (uint64, error) {
	data := b.Get(offsetKey)
	return unmarshalOffset(data)
}

// storeNextOffset updates the next free offset.
func storeNextOffset(b *bbolt.Bucket, next uint64) error {
	data := marshalOffset(next)
	return b.Put(offsetKey, data)
}

// loadMessage loads a message at a specific offset.
func loadMessage(
	m marshalkit.ValueMarshaler,
	b *bbolt.Bucket,
	offset uint64,
) (*envelope.Envelope, error) {
	k := marshalOffset(offset)
	v := b.Bucket(messagesKey).Get(k)

	var env envelope.Envelope
	if err := envelope.UnmarshalBinary(m, v, &env); err != nil {
		return nil, err
	}

	return &env, nil
}

// appendMessages writes messages to the database.
func appendMessages(
	b *bbolt.Bucket,
	next uint64,
	envelopes []*envelope.Envelope,
) (uint64, error) {
	messages, err := createBucket(b, messagesKey)
	if err != nil {
		return 0, err
	}

	for _, env := range envelopes {
		k := marshalOffset(next)
		v := envelope.MustMarshalBinary(env)

		if err := messages.Put(k, v); err != nil {
			return 0, err
		}

		next++
	}

	return next, nil
}
