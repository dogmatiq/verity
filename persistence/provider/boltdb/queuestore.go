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

var (
	// queueStoreBucketKey is the key for the bucket at the root of the
	// queuestore.
	queueStoreBucketKey = []byte("queuestore")

	// queueStoreItemsBucketKey is the key for a child bucket that contains each
	// queued message.
	//
	// The keys are the application-defined message ID. The values are
	// pb.QueueStoreItem values marshaled using protocol buffers.
	queueStoreItemsBucketKey = []byte("items")

	// queueStoreOrderBucketKey is the key for a child bucket that is used to
	// index the queued messages by their next-attempt time.
	//
	// The keys are the next-attempt time, represented as RFC3339Nano strings.
	// The values are buckets indicating which messages are due to be attempted
	// at that time.
	//
	// Within this further sub-bucket, the keys are the application-defined
	// message ID, and the values are always nil.
	queueStoreOrderBucketKey = []byte("order")
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

	// Note, we create all of the buckets before returning any error. That way,
	// if the transaction gets committed despite the error, all of the buckets
	// either exist or don't exist.
	store := bboltx.CreateBucketIfNotExists(
		t.actual,
		t.appKey,
		queueStoreBucketKey,
	)

	items := bboltx.CreateBucketIfNotExists(
		store,
		queueStoreItemsBucketKey,
	)

	order := bboltx.CreateBucketIfNotExists(
		store,
		queueStoreOrderBucketKey,
	)

	old := loadQueueStoreItem(items, i.ID())

	if uint64(i.Revision) != old.GetRevision() {
		return queuestore.ErrConflict
	}

	new, data := marshalQueueStoreItem(i, old)
	bboltx.Put(items, []byte(i.ID()), data)

	if new.GetNextAttemptAt() != old.GetNextAttemptAt() {
		if old != nil {
			removeQueueStoreOrder(order, old)
		}

		saveQueueStoreOrder(order, new)
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

	store, ok := bboltx.TryBucket(
		t.actual,
		t.appKey,
		queueStoreBucketKey,
	)
	if !ok {
		return queuestore.ErrConflict
	}

	items := bboltx.Bucket(
		store,
		queueStoreItemsBucketKey,
	)

	old := loadQueueStoreItem(items, i.ID())

	if uint64(i.Revision) != old.GetRevision() {
		return queuestore.ErrConflict
	}

	order := bboltx.Bucket(
		store,
		queueStoreOrderBucketKey,
	)

	bboltx.Delete(items, []byte(i.ID()))
	removeQueueStoreOrder(order, old)

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

	r.db.View(
		ctx,
		func(tx *bbolt.Tx) {
			store, exists := bboltx.TryBucket(
				tx,
				r.appKey,
				queueStoreBucketKey,
			)
			if !exists {
				return
			}

			items := bboltx.Bucket(
				store,
				queueStoreItemsBucketKey,
			)

			order := bboltx.Bucket(
				store,
				queueStoreOrderBucketKey,
			)

			orderCursor := order.Cursor()
			k, _ := orderCursor.First()

			for k != nil {
				idCursor := order.Bucket(k).Cursor()
				id, _ := idCursor.First()

				for id != nil {
					data := items.Get(id)
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
// representation with an incremented revision.
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

// saveQueueStoreOrder adds a record for i to the order bucket.
func saveQueueStoreOrder(order *bbolt.Bucket, i *pb.QueueStoreItem) {
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

// removeQueueStoreOrder removes the record for i from the order bucket.
func removeQueueStoreOrder(order *bbolt.Bucket, i *pb.QueueStoreItem) {
	id := i.GetEnvelope().GetMetaData().GetMessageId()

	bboltx.Delete(
		bboltx.Bucket(
			order,
			[]byte(i.NextAttemptAt),
		),
		[]byte(id),
	)
}
