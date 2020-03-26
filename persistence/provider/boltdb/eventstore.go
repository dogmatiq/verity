package boltdb

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence/eventstore"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/bbolt"
)

var (
	eventStoreBucketKey = []byte("eventstore")
	eventsBucketKey     = []byte("events")
	offsetKey           = []byte("offset")
)

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
	defer bboltx.Recover(&err)

	var end eventstore.Offset

	r.db.View(
		ctx,
		func(tx *bbolt.Tx) {
			if store, ok := bboltx.TryBucket(
				tx,
				r.appKey,
				eventStoreBucketKey,
			); ok {
				end = loadNextOffset(store)
			}
		},
	)

	if q.End == 0 || end < q.End {
		q.End = end
	}

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

	// Bail early without acquiring the lock if we're already at the end.
	if r.query.Begin >= r.query.End {
		return nil, false, nil
	}

	filtered := len(r.query.PortableNames) > 0

	// Execute a read-only transaction.
	r.db.View(
		ctx,
		func(tx *bbolt.Tx) {
			events := bboltx.Bucket(
				tx,
				r.appKey,
				eventStoreBucketKey,
				eventsBucketKey,
			)

			// Loop through the events in the store as per the query range.
			for r.query.Begin < r.query.End {
				// Bail if we're taking too long.
				bboltx.Must(ctx.Err())

				ev = loadEvent(events, r.query.Begin)
				r.query.Begin++

				// Skip over anything that doesn't match the type filter.
				//
				// TODO: improve filtering performance by using an "index
				// bucket" to search by portable type name, aggregate instance,
				// etc.
				if filtered {
					if _, ok := r.query.PortableNames[ev.Envelope.PortableName]; !ok {
						continue
					}
				}

				// We found a match.
				ok = true
				return
			}
		},
	)

	return ev, ok, nil
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	r.query.Begin = r.query.End
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
	v := b.Get(k)

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
	for _, env := range envelopes {
		k := marshalOffset(o)
		v, err := proto.Marshal(env)
		bboltx.Must(err)

		bboltx.Put(b, k, v)
		o++
	}

	return o
}
