package boltdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/bbolt"
)

// SaveEvent persists an event in the application's event store.
//
// It returns the event's offset.
func (t *transaction) SaveEvent(
	ctx context.Context,
	env *envelopespec.Envelope,
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
	saveEventStoreParcel(events, o, env)
	storeNextOffset(store, o+1)

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
) (p *eventstore.Parcel, ok bool, err error) {
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

				p, exists = loadEventStoreParcel(events, r.query.MinOffset)
				ok = exists && r.query.IsMatch(p)

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

// loadEventStoreParcel loads an event at a specific offset.
func loadEventStoreParcel(
	events *bbolt.Bucket,
	o eventstore.Offset,
) (*eventstore.Parcel, bool) {
	k := marshalOffset(o)
	v := events.Get(k)

	if v == nil {
		return nil, false
	}

	var env envelopespec.Envelope
	bboltx.Must(proto.Unmarshal(v, &env))

	return &eventstore.Parcel{
		Offset:   o,
		Envelope: &env,
	}, true
}

// saveEventStoreParcel writes an event to the store at a specific offset.
func saveEventStoreParcel(
	events *bbolt.Bucket,
	o eventstore.Offset,
	env *envelopespec.Envelope,
) {
	k := marshalOffset(o)
	v, err := proto.Marshal(env)
	bboltx.Must(err)
	bboltx.Put(events, k, v)
}
