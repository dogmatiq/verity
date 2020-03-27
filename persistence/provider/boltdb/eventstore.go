package boltdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence/eventstore"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/bbolt"
)

// SaveEvents persists events in the application's event store.
//
// It returns the next free offset in the store.
func (t *transaction) SaveEvents(
	ctx context.Context,
	envelopes []*envelopespec.Envelope,
) (_ eventstore.Offset, err error) {
	defer bboltx.Recover(&err)

	if err := t.begin(ctx); err != nil {
		return 0, err
	}

	store := bboltx.CreateBucketIfNotExists(
		t.actual,
		t.appKey,
		eventStoreBucketKey,
	)

	events := bboltx.CreateBucketIfNotExists(
		store,
		eventsBucketKey,
	)

	o := loadNextOffset(store)
	o = saveEvents(events, o, envelopes)
	storeNextOffset(store, o)

	return o, nil
}

// eventStoreRepository is an implementation of eventstore.Repository that
// stores events in a BoltDB database.
type eventStoreRepository struct {
	db     *database
	appKey []byte
}

// QueryEvents queries events in the repository.
func (r *eventStoreRepository) QueryEvents(
	ctx context.Context,
	q eventstore.Query,
) (_ eventstore.Result, err error) {
	return &eventStoreResult{
		db:     r.db,
		appKey: r.appKey,
		query:  q,
	}, nil
}

// eventStoreResult is an implementation of eventstore.Result for the BoltDB
// event store.
type eventStoreResult struct {
	db     *database
	appKey []byte
	query  eventstore.Query
}

// Next returns the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventStoreResult) Next(
	ctx context.Context,
) (ev *eventstore.Event, ok bool, err error) {
	defer bboltx.Recover(&err)

	// Execute a read-only transaction.
	r.db.View(
		ctx,
		func(tx *bbolt.Tx) {
			events, exists := bboltx.TryBucket(
				tx,
				r.appKey,
				eventStoreBucketKey,
				eventsBucketKey,
			)

			for exists && !ok {
				bboltx.Must(ctx.Err()) // Bail if we're taking too long.

				ev, exists = loadEvent(events, r.query.MinOffset)
				ok = exists && r.query.IsMatch(ev)

				r.query.MinOffset++
			}
		},
	)

	return
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	r.query.MinOffset = math.MaxUint64
	return nil
}

var (
	eventStoreBucketKey = []byte("eventstore")
	eventsBucketKey     = []byte("events")
	offsetKey           = []byte("offset")
)

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
func loadEvent(b *bbolt.Bucket, o eventstore.Offset) (*eventstore.Event, bool) {
	k := marshalOffset(o)
	v := b.Get(k)

	if v == nil {
		return nil, false
	}

	var env envelopespec.Envelope
	bboltx.Must(proto.Unmarshal(v, &env))

	return &eventstore.Event{
		Offset:   o,
		Envelope: &env,
	}, true
}

// saveEvents writes events to the database.
func saveEvents(
	b *bbolt.Bucket,
	o eventstore.Offset,
	envelopes []*envelopespec.Envelope,
) eventstore.Offset {
	for _, env := range envelopes {
		k := marshalOffset(o)
		v, err := proto.Marshal(env)
		bboltx.Must(err)

		bboltx.Put(b, k, v)
		o++
	}

	return o
}
