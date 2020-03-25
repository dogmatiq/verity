package boltdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence/eventstore"
	"github.com/dogmatiq/marshalkit"
	"go.etcd.io/bbolt"
)

var (
	eventStoreBucketKey = []byte("eventstore")
	offsetKey           = []byte("offset")
	eventsKey           = []byte("events")
)

// eventStoreRepository is an implementation of eventstore.Repository that
// stores events in memory.
type eventStoreRepository struct {
	db     *bbolt.DB
	appKey []byte
}

// QueryEvents queries events in the repository.
func (r *eventStoreRepository) QueryEvents(
	ctx context.Context,
	q eventstore.Query,
) (_ eventstore.Result, err error) {
	defer bboltx.Recover(&err)

	if q.Types != nil && q.Types.Len() == 0 {
		panic("q.Types must be nil or otherwise contain at least one event type")
	}

	tx := bboltx.BeginRead(r.db)
	defer tx.Rollback()

	var end eventstore.Offset
	if b, ok := bboltx.TryBucket(tx, r.appKey, eventStoreBucketKey); ok {
		end = loadNextOffset(b)
	}

	if q.End == 0 || end < q.End {
		q.End = end
	}

	return &eventStoreResult{
		query:  q,
		db:     r.db,
		appKey: r.appKey,
	}, nil
}

// eventStoreResult is an implementation of eventstore.Result for the in-memory
// event store.
type eventStoreResult struct {
	query  eventstore.Query
	db     *bbolt.DB
	appKey []byte
	// event  *eventstore.Event
	// done   bool
}

// Next advances to the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventStoreResult) Next() bool {
	if r.query.Begin < r.query.End {
		r.query.Begin++
		return true
	}

	return false
}

// Next returns current event in the result.
func (r *eventStoreResult) Get(ctx context.Context) (_ *eventstore.Event, err error) {
	defer bboltx.Recover(&err)

	if r.query.Begin > r.query.End {
		return nil, errors.New("no more results")
	}

	tx := bboltx.BeginRead(r.db)
	defer tx.Rollback()

	b := bboltx.XBucket(tx, r.appKey, eventStoreBucketKey)

	o := r.query.Begin - 1

	return nil, errors.New("not implemented")
	// if r.done {
	// 	return nil, errors.New("no more results")
	// }

	// if r.event == nil {
	// 	panic("Next() must be called before calling Get()")
	// }

	// return r.event, nil
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	// r.db = nil
	// r.event = nil
	// r.done = true

	return nil
}

// marshalOffset marshals a stream offset to its binary representation.
func marshalOffset(offset eventstore.Offset) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(offset))
	return data
}

// unmarshalOffset unmarshals a stream offset from its binary representation.
func unmarshalOffset(data []byte) eventstore.Offset {
	n := len(data)

	switch n {
	case 0:
		return 0
	case 8:
		return eventstore.Offset(
			binary.BigEndian.Uint64(data),
		)
	default:
		panic(bboltx.PanicSentinel{
			Cause: fmt.Errorf("offset data is corrupt, expected 8 bytes, got %d", n),
		})
	}
}

// loadNextOffset returns the next free offset.
func loadNextOffset(b *bbolt.Bucket) eventstore.Offset {
	data := b.Get(offsetKey)
	return unmarshalOffset(data)
}

// storeNextOffset updates the next free offset.
func storeNextOffset(b *bbolt.Bucket, next eventstore.Offset) {
	data := marshalOffset(next)
	bboltx.Put(b, offsetKey, data)
}

// loadEvent loads an event at a specific offset.
func loadEvent(
	m marshalkit.ValueMarshaler,
	b *bbolt.Bucket,
	offset eventstore.Offset,
) *eventstore.Event {
	k := marshalOffset(offset)
	v := b.Bucket(eventsKey).Get(k)

	var env envelope.Envelope
	bboltx.Must(envelope.UnmarshalBinary(m, v, &env))

	return &env
}

// appendEvents writes events to the database.
func appendEvents(
	b *bbolt.Bucket,
	next eventstore.Offset,
	envelopes []*envelope.Envelope,
) eventstore.Offset {
	events := bboltx.CreateBucketIfNotExists(b, eventsKey)

	for _, env := range envelopes {
		k := marshalOffset(next)
		v := envelope.MustMarshalBinary(env)
		bboltx.Put(events, k, v)
		next++
	}

	return next
}
