package boltdb

import (
	"context"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/bbolt"
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

	// eventOffsetsBucketKey is the key for a child bucket that allows retrieval
	// of event offsets by their message ID.
	//
	// The keys are the message IDs. The values are event offsets encoded as
	// 8-byte big-endian packets.
	eventOffsetsBucketKey = []byte("offsets")

	// eventNextOffsetKey is the key of a value within the root bucket that
	// contains the next unused offset encoded as 8-byte big-endian packet.
	eventNextOffsetKey = []byte("offset")
)

// NextEventOffset returns the next "unused" offset.
func (ds *dataStore) NextEventOffset(
	ctx context.Context,
) (_ uint64, err error) {
	var next uint64

	bboltx.View(
		ds.db,
		func(tx *bbolt.Tx) {
			if events, ok := bboltx.TryBucket(
				tx,
				ds.appKey,
				eventBucketKey,
			); ok {
				next = unmarshalUint64(
					events.Get(eventNextOffsetKey),
				)
			}
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
	ctx context.Context,
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
//
// m is ID of a "barrier" message. If supplied, the results are limited to
// events with higher offsets than the barrier message. If the message
// cannot be found, UnknownMessageError is returned.
func (ds *dataStore) LoadEventsBySource(
	ctx context.Context,
	hk, id, m string,
) (persistence.EventResult, error) {
	var offset uint64

	if m != "" {
		o, err := ds.offsetOf(m)
		if err != nil {
			return nil, err
		}
		offset = o + 1 // start with the message AFTER the barrier message.
	}

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

// offsetOf returns the offset of the message with the given ID.
func (ds *dataStore) offsetOf(id string) (uint64, error) {
	var offset uint64

	err := ds.db.View(
		func(tx *bbolt.Tx) error {
			if offsets, ok := bboltx.TryBucket(
				tx,
				ds.appKey,
				eventBucketKey,
				eventOffsetsBucketKey,
			); ok {
				if v := offsets.Get([]byte(id)); v != nil {
					offset = unmarshalUint64(v)
					return nil
				}
			}

			return persistence.UnknownMessageError{
				MessageID: id,
			}
		},
	)

	return offset, err
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
			envelopes, ok := bboltx.TryBucket(
				tx,
				r.appKey,
				eventBucketKey,
				eventEnvelopesBucketKey,
			)

			if !ok {
				// The envelopes bucket doesn't exist at all, so there can't be
				// any events.
				return
			}

			for {
				// Bail if we're taking too long to search through a large
				// number of events.
				bboltx.Must(ctx.Err())

				env, ok := loadEventEnvelope(envelopes, r.offset)
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
	events := bboltx.CreateBucketIfNotExists(
		c.root,
		eventBucketKey,
	)

	envelopes := bboltx.CreateBucketIfNotExists(
		events,
		eventEnvelopesBucketKey,
	)

	offsets := bboltx.CreateBucketIfNotExists(
		events,
		eventOffsetsBucketKey,
	)

	id := op.Envelope.GetMessageId()

	// Load the next free offset.
	offset := unmarshalUint64(
		events.Get(eventNextOffsetKey),
	)

	// Save the envelope.
	saveEventEnvelope(envelopes, offset, op.Envelope)

	// Save the message ID -> offset index.
	bboltx.Put(
		offsets,
		[]byte(id),
		marshalUint64(offset),
	)

	// Update the next free offset.
	bboltx.Put(
		events,
		eventNextOffsetKey,
		marshalUint64(offset+1),
	)

	// Add the offset to the result.
	if c.result.EventOffsets == nil {
		c.result.EventOffsets = map[string]uint64{}
	}

	c.result.EventOffsets[id] = offset

	return nil
}

// loadEventEnvelope loads the tiem at the given offset from b.
func loadEventEnvelope(b *bbolt.Bucket, offset uint64) (*envelopespec.Envelope, bool) {
	k := marshalUint64(offset)
	v := b.Get(k)

	if v == nil {
		return nil, false
	}

	var env envelopespec.Envelope
	bboltx.Must(proto.Unmarshal(v, &env))

	return &env, true
}

// saveEventEnvelope saves an envelope to b at the given offset.
func saveEventEnvelope(b *bbolt.Bucket, offset uint64, env *envelopespec.Envelope) {
	k := marshalUint64(offset)
	v, err := proto.Marshal(env)

	bboltx.Must(err)
	bboltx.Put(b, k, v)
}
