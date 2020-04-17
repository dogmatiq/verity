package boltdb

import (
	"context"
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
	saveEventStoreItem(events, o, env)
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
) (i *eventstore.Item, ok bool, err error) {
	defer bboltx.Recover(&err)

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

				i, exists = loadEventStoreItem(events, r.query.MinOffset)
				ok = exists && r.query.IsMatch(i)

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

// marshalEventStoreOffset marshals a stream offset to its binary representation.
func marshalEventStoreOffset(offset eventstore.Offset) []byte {
	return marshalUint64(uint64(offset))
}

// unmarshalEventStoreOffset unmarshals a stream offset from its binary representation.
func unmarshalEventStoreOffset(data []byte) eventstore.Offset {
	return eventstore.Offset(unmarshalUint64(data))
}

// loadNextOffset returns the next free offset.
func loadNextOffset(b *bbolt.Bucket) eventstore.Offset {
	data := b.Get(offsetKey)
	return unmarshalEventStoreOffset(data)
}

// storeNextOffset updates the next free offset.
func storeNextOffset(b *bbolt.Bucket, next eventstore.Offset) {
	data := marshalEventStoreOffset(next)
	bboltx.Put(b, offsetKey, data)
}

// loadEventStoreItem loads the item at a specific offset.
func loadEventStoreItem(
	events *bbolt.Bucket,
	o eventstore.Offset,
) (*eventstore.Item, bool) {
	k := marshalEventStoreOffset(o)
	v := events.Get(k)

	if v == nil {
		return nil, false
	}

	var env envelopespec.Envelope
	bboltx.Must(proto.Unmarshal(v, &env))

	return &eventstore.Item{
		Offset:   o,
		Envelope: &env,
	}, true
}

// saveEventStoreItem writes an event to the store at a specific offset.
func saveEventStoreItem(
	events *bbolt.Bucket,
	o eventstore.Offset,
	env *envelopespec.Envelope,
) {
	k := marshalEventStoreOffset(o)
	v, err := proto.Marshal(env)
	bboltx.Must(err)
	bboltx.Put(events, k, v)
}
