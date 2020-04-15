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
// i.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// is not saved and ErrConflict is returned.
func (t *transaction) SaveMessageToQueue(
	ctx context.Context,
	i *queuestore.Item,
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

	old := loadQueueStoreItem(messages, i.ID())

	if uint64(i.Revision) != old.GetRevision() {
		return queuestore.ErrConflict
	}

	new, data := marshalQueueStoreItem(i, old)
	bboltx.Put(messages, []byte(i.ID()), data)

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
// i.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// remains on the queue and ErrConflict is returned.
func (t *transaction) RemoveMessageFromQueue(
	ctx context.Context,
	i *queuestore.Item,
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

	old := loadQueueStoreItem(messages, i.ID())

	if uint64(i.Revision) != old.GetRevision() {
		return queuestore.ErrConflict
	}

	order := bboltx.Bucket(
		t.actual,
		t.appKey,
		queueBucketKey,
		orderBucketKey,
	)

	bboltx.Delete(messages, []byte(i.ID()))
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
) (_ []*queuestore.Item, err error) {
	defer bboltx.Recover(&err)

	var result []*queuestore.Item

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
					i := unmarshalQueueStoreItem(data)
					result = append(result, i)

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

// unmarshalQueueStoreItem unmarshals a queuestore.Item from its binary
// representation.
func unmarshalQueueStoreItem(data []byte) *queuestore.Item {
	var i pb.QueueStoreItem
	bboltx.Must(proto.Unmarshal(data, &i))

	next, err := time.Parse(time.RFC3339Nano, i.NextAttemptAt)
	bboltx.Must(err)

	return &queuestore.Item{
		Revision:      queuestore.Revision(i.Revision),
		FailureCount:  uint(i.FailureCount),
		NextAttemptAt: next,
		Envelope:      i.Envelope,
	}
}

// marshalQueueStoreItem marshals a queuestore.Item to its binary
// representation.
func marshalQueueStoreItem(
	i *queuestore.Item,
	old *pb.QueueStoreItem,
) (*pb.QueueStoreItem, []byte) {
	new := &pb.QueueStoreItem{
		Revision:      uint64(i.Revision + 1),
		FailureCount:  uint64(i.FailureCount),
		NextAttemptAt: i.NextAttemptAt.Format(time.RFC3339Nano),
		Envelope:      old.GetEnvelope(),
	}

	// Only use user-supplied envelope if there's no old one.
	// This satisfies the requirement that updates only modify meta-data.
	if new.Envelope == nil {
		new.Envelope = i.Envelope
	}

	data, err := proto.Marshal(new)
	bboltx.Must(err)

	return new, data
}

// loadQueueStoreItem loads an item from the queue.
func loadQueueStoreItem(messages *bbolt.Bucket, id string) *pb.QueueStoreItem {
	data := messages.Get([]byte(id))
	if data == nil {
		return nil
	}

	i := &pb.QueueStoreItem{}
	err := proto.Unmarshal(data, i)
	bboltx.Must(err)

	return i
}

// saveQueueOrder adds a record for i to the order bucket.
func saveQueueOrder(order *bbolt.Bucket, i *pb.QueueStoreItem) {
	id := i.GetEnvelope().GetMetaData().GetMessageId()

	bboltx.Put(
		bboltx.CreateBucketIfNotExists(
			order,
			[]byte(i.NextAttemptAt),
		),
		[]byte(id),
		nil,
	)
}

// removeQueueOrder removes the record for i from the order bucket.
func removeQueueOrder(order *bbolt.Bucket, i *pb.QueueStoreItem) {
	id := i.GetEnvelope().GetMetaData().GetMessageId()

	bboltx.Delete(
		bboltx.Bucket(
			order,
			[]byte(i.NextAttemptAt),
		),
		[]byte(id),
	)
}
