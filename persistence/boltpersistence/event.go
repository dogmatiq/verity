package boltpersistence

import (
	"context"

	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/verity/internal/x/bboltx"
	"github.com/dogmatiq/verity/persistence"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

var (
	// eventBucketKey is the key for the root bucket for events.
	eventBucketKey = []byte("event")

	// eventEnvelopesBucketKey is the key for a child bucket that contains each
	// event's envelope.
	//
	// The keys are the event offsets encoded as 8-byte big-endian packets. The
	// values are envelopespec.Envelope values marshaled using protocol buffers.
	eventEnvelopesBucketKey = []byte("envelopes")

	// eventNextOffsetKey is the key of a value within the root bucket that
	// contains the next unused offset encoded as 8-byte big-endian packet.
	eventNextOffsetKey = []byte("offset")
)

// NextEventOffset returns the next "unused" offset.
func (ds *dataStore) NextEventOffset(
	context.Context,
) (_ uint64, err error) {
	var next uint64

	bboltx.View(
		ds.db,
		func(tx *bbolt.Tx) {
			next = unmarshalUint64(
				bboltx.GetPath(
					tx,
					ds.appKey,
					eventBucketKey,
					eventNextOffsetKey,
				),
			)
		},
	)

	return next, nil
}

// LoadEventsByType loads events that match a specific set of message types.
//
// f is the set of message types to include in the result. The keys of f are
// the "portable type name" produced when the events are marshaled.
//
// o specifies the (inclusive) lower-bound of the offset range to include in
// the results.
func (ds *dataStore) LoadEventsByType(
	_ context.Context,
	f map[string]struct{},
	o uint64,
) (persistence.EventResult, error) {
	return &eventResult{
		db:     ds.db,
		appKey: ds.appKey,
		offset: o,
		pred: func(env *envelopespec.Envelope) bool {
			_, ok := f[env.GetPortableName()]
			return ok
		},
	}, nil
}

// LoadEventsBySource loads the events produced by a specific handler.
//
// hk is the handler's identity key.
//
// id is the instance ID, which must be empty if the handler type does not
// use instances.
func (ds *dataStore) LoadEventsBySource(
	_ context.Context,
	hk, id string,
) (persistence.EventResult, error) {
	var offset uint64

	return &eventResult{
		db:     ds.db,
		appKey: ds.appKey,
		offset: offset,
		pred: func(env *envelopespec.Envelope) bool {
			return env.GetSourceHandler().GetKey() == hk &&
				env.GetSourceInstanceId() == id
		},
	}, nil
}

// eventResult is an implementation of persistence.EventResult for BoltDB.
type eventResult struct {
	db     *bbolt.DB
	appKey []byte
	offset uint64
	pred   func(*envelopespec.Envelope) bool
}

// Next returns the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventResult) Next(
	ctx context.Context,
) (_ persistence.Event, _ bool, err error) {
	defer bboltx.Recover(&err)

	var match persistence.Event

	bboltx.View(
		r.db,
		func(tx *bbolt.Tx) {
			root, ok := bboltx.TryBucket(tx, r.appKey)

			if !ok {
				// The root bucket doesn't exist at all, so there can't be
				// any events.
				return
			}

			for {
				// Bail if we're taking too long to search through a large
				// number of events.
				bboltx.Must(ctx.Err())

				env, ok := loadEventEnvelope(root, r.offset)
				if !ok {
					// There is no event at this offset, we've seen everything.
					return
				}

				ok = r.pred(env)
				r.offset++

				if ok {
					// We got a match, bail so we can return it.
					match.Offset = r.offset - 1
					match.Envelope = env
					return
				}
			}
		},
	)

	return match, match.Envelope != nil, nil
}

// Close closes the cursor.
func (r *eventResult) Close() error {
	return nil
}

// VisitSaveEvent applies the changes in a "SaveEvent" operation to the
// database.
func (c *committer) VisitSaveEvent(
	_ context.Context,
	op persistence.SaveEvent,
) error {
	id := op.Envelope.GetMessageId()

	// Fetch the event's offset.
	offset := incrementEventOffset(c.root)

	// Save the envelope.
	saveEventEnvelope(c.root, offset, op.Envelope)

	// Add the offset to the result.
	if c.result.EventOffsets == nil {
		c.result.EventOffsets = map[string]uint64{}
	}
	c.result.EventOffsets[id] = offset

	return nil
}

// incrementEventOffset increments the next free event offset.
// It returns the previous value, to be used for the next event.
func incrementEventOffset(root *bbolt.Bucket) uint64 {
	data := bboltx.GetPath(
		root,
		eventBucketKey,
		eventNextOffsetKey,
	)

	offset := unmarshalUint64(data)

	bboltx.PutPath(
		root,
		marshalUint64(offset+1),
		eventBucketKey,
		eventNextOffsetKey,
	)

	return offset
}

// loadEventEnvelope loads the tiem at the given offset from b.
func loadEventEnvelope(root *bbolt.Bucket, offset uint64) (*envelopespec.Envelope, bool) {
	k := marshalUint64(offset)
	v := bboltx.GetPath(
		root,
		eventBucketKey,
		eventEnvelopesBucketKey,
		k,
	)

	if v == nil {
		return nil, false
	}

	var env envelopespec.Envelope
	bboltx.Must(proto.Unmarshal(v, &env))

	return &env, true
}

// saveEventEnvelope saves an envelope to b at the given offset.
func saveEventEnvelope(root *bbolt.Bucket, offset uint64, env *envelopespec.Envelope) {
	k := marshalUint64(offset)
	v, err := proto.Marshal(env)
	bboltx.Must(err)

	bboltx.PutPath(
		root,
		v,
		eventBucketKey,
		eventEnvelopesBucketKey,
		k,
	)
}
