package boltdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence/eventstore"
	"github.com/golang/protobuf/proto"
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
	query   eventstore.Query
	started bool
	db      *bbolt.DB
	appKey  []byte
}

// Next advances to the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventStoreResult) Next() bool {
	if r.query.Begin < r.query.End {
		r.started = true
		r.query.Begin++

		return true
	}

	return false
}

// Next returns current event in the result.
func (r *eventStoreResult) Get(ctx context.Context) (_ *eventstore.Event, err error) {
	defer bboltx.Recover(&err)

	if !r.started {
		panic("Next() must be called before calling Get()")
	}

	if r.query.Begin > r.query.End {
		return nil, errors.New("no more results")
	}

	tx := bboltx.BeginRead(r.db)
	defer tx.Rollback()

	return loadEvent(
		bboltx.Bucket(tx, r.appKey, eventStoreBucketKey),
		r.query.Begin-1,
	), nil
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	r.query.Begin = r.query.End
	r.db = nil

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
func loadEvent(b *bbolt.Bucket, o eventstore.Offset) *eventstore.Event {
	k := marshalOffset(o)
	v := b.Bucket(eventsKey).Get(k)

	var env envelopespec.Envelope
	bboltx.Must(proto.Unmarshal(v, &env))

	return &eventstore.Event{
		Offset:   o,
		Envelope: &env,
	}
}

// saveEvents writes events to the database.
func saveEvents(
	b *bbolt.Bucket,
	o eventstore.Offset,
	envelopes []*envelopespec.Envelope,
) eventstore.Offset {
	events := bboltx.CreateBucketIfNotExists(b, eventsKey)

	for _, env := range envelopes {
		k := marshalOffset(o)
		v, err := proto.Marshal(env)
		bboltx.Must(err)

		bboltx.Put(events, k, v)
		o++
	}

	return o
}
