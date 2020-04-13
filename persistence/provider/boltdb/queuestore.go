package boltdb

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence/provider/boltdb/internal/pb"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/bbolt"
)

// SaveMessageToQueue persists a message to the application's message queue.
//
// If the message is already on the queue its meta-data is updated.
//
// p.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// is not saved and ErrConflict is returned.
func (t *transaction) SaveMessageToQueue(
	ctx context.Context,
	p *queuestore.Parcel,
) (err error) {
	defer bboltx.Recover(&err)

	if err := t.begin(ctx); err != nil {
		return err
	}

	messages := bboltx.CreateBucketIfNotExists(
		t.actual,
		t.appKey,
		queueBucketKey,
		messagesBucketKey,
	)

	old := loadQueueStoreParcel(messages, p.ID())

	if uint64(p.Revision) != old.GetRevision() {
		return queuestore.ErrConflict
	}

	new, data := marshalQueueStoreParcel(p, old)
	bboltx.Put(messages, []byte(p.ID()), data)

	if new.GetNextAttemptAt() != old.GetNextAttemptAt() {
		order := bboltx.CreateBucketIfNotExists(
			t.actual,
			t.appKey,
			queueBucketKey,
			orderBucketKey,
		)

		if old != nil {
			removeQueueOrder(order, old)
		}

		saveQueueOrder(order, new)
	}

	return nil
}

// RemoveMessageFromQueue removes a specific message from the application's
// message queue.
//
// p.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// remains on the queue and ErrConflict is returned.
func (t *transaction) RemoveMessageFromQueue(
	ctx context.Context,
	p *queuestore.Parcel,
) (err error) {
	defer bboltx.Recover(&err)

	if err := t.begin(ctx); err != nil {
		return err
	}

	messages, ok := bboltx.TryBucket(
		t.actual,
		t.appKey,
		queueBucketKey,
		messagesBucketKey,
	)
	if !ok {
		return queuestore.ErrConflict
	}

	old := loadQueueStoreParcel(messages, p.ID())

	if uint64(p.Revision) != old.GetRevision() {
		return queuestore.ErrConflict
	}

	order := bboltx.Bucket(
		t.actual,
		t.appKey,
		queueBucketKey,
		orderBucketKey,
	)

	bboltx.Delete(messages, []byte(p.ID()))
	removeQueueOrder(order, old)

	return nil
}

// queueStoreRepository is an implementation of queuestore.Repository that
// stores queued messages in a BoltDB database.
type queueStoreRepository struct {
	db     *database
	appKey []byte
}

// LoadQueueMessages loads the next n messages from the queue.
func (r *queueStoreRepository) LoadQueueMessages(
	ctx context.Context,
	n int,
) (_ []*queuestore.Parcel, err error) {
	defer bboltx.Recover(&err)

	var result []*queuestore.Parcel

	// Execute a read-only transaction.
	r.db.View(
		ctx,
		func(tx *bbolt.Tx) {
			order, exists := bboltx.TryBucket(
				tx,
				r.appKey,
				queueBucketKey,
				orderBucketKey,
			)
			if !exists {
				return
			}

			messages := bboltx.Bucket(
				tx,
				r.appKey,
				queueBucketKey,
				messagesBucketKey,
			)

			orderCursor := order.Cursor()
			k, _ := orderCursor.First()

			for k != nil {
				idCursor := order.Bucket(k).Cursor()
				id, _ := idCursor.First()

				for id != nil {
					data := messages.Get(id)
					p := unmarshalQueueStoreParcel(data)
					result = append(result, p)

					if len(result) == n {
						return
					}

					id, _ = idCursor.Next()
				}

				k, _ = orderCursor.Next()
			}
		},
	)

	return result, nil
}

var (
	queueBucketKey    = []byte("queue")
	messagesBucketKey = []byte("messages")
	orderBucketKey    = []byte("order")
)

// unmarshalQueueStoreParcel unmarshals a queuestore.Parcel from its binary
// representation.
func unmarshalQueueStoreParcel(data []byte) *queuestore.Parcel {
	var p pb.QueueStoreParcel
	bboltx.Must(proto.Unmarshal(data, &p))

	next, err := time.Parse(time.RFC3339Nano, p.NextAttemptAt)
	bboltx.Must(err)

	return &queuestore.Parcel{
		Revision:      queuestore.Revision(p.Revision),
		FailureCount:  uint(p.FailureCount),
		NextAttemptAt: next,
		Envelope:      p.Envelope,
	}
}

// marshalQueueStoreParcel marshals a queuestore.Parcel to its binary
// representation.
func marshalQueueStoreParcel(
	p *queuestore.Parcel,
	old *pb.QueueStoreParcel,
) (*pb.QueueStoreParcel, []byte) {
	new := &pb.QueueStoreParcel{
		Revision:      uint64(p.Revision + 1),
		FailureCount:  uint64(p.FailureCount),
		NextAttemptAt: p.NextAttemptAt.Format(time.RFC3339Nano),
		Envelope:      old.GetEnvelope(),
	}

	// Only use user-supplied envelope if there's no old one.
	// This satisfies the requirement that updates only modify meta-data.
	if new.Envelope == nil {
		new.Envelope = p.Envelope
	}

	data, err := proto.Marshal(new)
	bboltx.Must(err)

	return new, data
}

// loadQueueStoreParcel loads the protobuf representation of a queuestore.Parcel.
func loadQueueStoreParcel(messages *bbolt.Bucket, id string) *pb.QueueStoreParcel {
	data := messages.Get([]byte(id))
	if data == nil {
		return nil
	}

	p := &pb.QueueStoreParcel{}
	err := proto.Unmarshal(data, p)
	bboltx.Must(err)

	return p
}

// saveQueueOrder adds a record for p to the order bucket.
func saveQueueOrder(order *bbolt.Bucket, p *pb.QueueStoreParcel) {
	id := p.GetEnvelope().GetMetaData().GetMessageId()

	bboltx.Put(
		bboltx.CreateBucketIfNotExists(
			order,
			[]byte(p.NextAttemptAt),
		),
		[]byte(id),
		nil,
	)
}

// removeQueueOrder removes the record for p from the order bucket.
func removeQueueOrder(order *bbolt.Bucket, p *pb.QueueStoreParcel) {
	id := p.GetEnvelope().GetMetaData().GetMessageId()

	bboltx.Delete(
		bboltx.Bucket(
			order,
			[]byte(p.NextAttemptAt),
		),
		[]byte(id),
	)
}
