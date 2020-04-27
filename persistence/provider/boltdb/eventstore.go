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

var (
	// eventStoreBucketKey is the key for the bucket at the root of the
	// eventstore.
	eventStoreBucketKey = []byte("eventstore")

	// eventStoreItemsBucketKey is the key for a child bucket that contains each
	// event.
	//
	// The keys are the event offsets encoded as 8-byte big-endian packets. The
	// values are envelopespec. Envelope values marshaled using protocol
	// buffers.
	eventStoreItemsBucketKey = []byte("items")

	// eventStoreNextOffsetKey is the key of a value within the root bucket that
	// contains the next unused offset, again encoded as 8-byte big-endian
	// packet.
	eventStoreNextOffsetKey = []byte("offset")
)

// SaveEvent persists an event in the application's event store.
//
// It returns the event's offset.
func (t *transaction) SaveEvent(
	ctx context.Context,
	env *envelopespec.Envelope,
) (_ uint64, err error) {
	defer bboltx.Recover(&err)

	if err := t.begin(ctx); err != nil {
		return 0, err
	}

	store := bboltx.CreateBucketIfNotExists(
		t.actual,
		t.appKey,
		eventStoreBucketKey,
	)

	items := bboltx.CreateBucketIfNotExists(
		store,
		eventStoreItemsBucketKey,
	)

	o := unmarshalUint64(
		store.Get(eventStoreNextOffsetKey),
	)

	saveEventStoreItem(items, o, env)

	bboltx.Put(
		store,
		eventStoreNextOffsetKey,
		marshalUint64(o+1),
	)

	return o, nil
}

// eventStoreRepository is an implementation of eventstore.Repository that
// stores events in a BoltDB database.
type eventStoreRepository struct {
	db     *database
	appKey []byte
}

// NextEventOffset returns the next "unused" offset within the store.
func (r *eventStoreRepository) NextEventOffset(
	ctx context.Context,
) (next uint64, err error) {
	defer bboltx.Recover(&err)

	r.db.View(
		ctx,
		func(tx *bbolt.Tx) {
			store, exists := bboltx.TryBucket(
				tx,
				r.appKey,
				eventStoreBucketKey,
			)

			if exists {
				next = unmarshalUint64(
					store.Get(eventStoreNextOffsetKey),
				)
			}
		},
	)

	return next, err
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
			items, exists := bboltx.TryBucket(
				tx,
				r.appKey,
				eventStoreBucketKey,
				eventStoreItemsBucketKey,
			)

			for exists && !ok {
				bboltx.Must(ctx.Err()) // Bail if we're taking too long.

				i, exists = loadEventStoreItem(items, r.query.MinOffset)
				ok = exists && r.query.IsMatch(i)

				r.query.MinOffset++
			}
		},
	)

	return i, ok, err
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	r.query.MinOffset = math.MaxUint64
	return nil
}

// loadEventStoreItem loads the item at a specific offset.
func loadEventStoreItem(
	events *bbolt.Bucket,
	o uint64,
) (*eventstore.Item, bool) {
	k := marshalUint64(o)
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
	o uint64,
	env *envelopespec.Envelope,
) {
	k := marshalUint64(o)
	v, err := proto.Marshal(env)
	bboltx.Must(err)
	bboltx.Put(events, k, v)
}
