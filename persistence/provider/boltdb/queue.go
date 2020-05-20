package boltdb

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/boltdb/internal/pb"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/bbolt"
)

var (
	// queueBucketKey is the key for the root bucket for the message queue.
	queueBucketKey = []byte("queue")

	// queueItemsBucketKey is the key for a child bucket that contains each
	// queued message.
	//
	// The keys are the application-defined message ID. The values are
	// pb.QueueItem values marshaled using protocol buffers.
	queueItemsBucketKey = []byte("items")

	// queueOrderBucketKey is the key for a child bucket that is used to index
	// the queued messages by their next-attempt time.
	//
	// The keys are the next-attempt time, represented as RFC3339Nano strings.
	// The values are buckets indicating which messages are due to be attempted
	// at that time.
	//
	// Within this further sub-bucket, the keys are the application-defined
	// message ID, and the values are always nil. This allows representation of
	// multiple messages with the same next-attempt time.
	queueOrderBucketKey = []byte("order")
)

// LoadQueueMessages loads the next n messages from the queue.
func (ds *dataStore) LoadQueueMessages(
	ctx context.Context,
	n int,
) (_ []*queuestore.Item, err error) {
	defer bboltx.Recover(&err)

	var result []*queuestore.Item

	bboltx.View(
		ds.db,
		func(tx *bbolt.Tx) {
			queue, ok := bboltx.TryBucket(
				tx,
				ds.appKey,
				queueBucketKey,
			)
			if !ok {
				return
			}

			items := bboltx.Bucket(
				queue,
				queueItemsBucketKey,
			)

			order := bboltx.Bucket(
				queue,
				queueOrderBucketKey,
			)

			orderCursor := order.Cursor()
			k, _ := orderCursor.First()

			for k != nil {
				idCursor := order.Bucket(k).Cursor()
				id, _ := idCursor.First()

				for id != nil {
					data := items.Get(id)
					item := unmarshalQueueItem(data)
					result = append(result, item)

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

// VisitSaveQueueItem applies the changes in a "SaveQueueItem" operation to the
// database.
func (c *committer) VisitSaveQueueItem(
	ctx context.Context,
	op persistence.SaveQueueItem,
) error {
	queue := bboltx.CreateBucketIfNotExists(
		c.root,
		queueBucketKey,
	)

	items := bboltx.CreateBucketIfNotExists(
		queue,
		queueItemsBucketKey,
	)

	order := bboltx.CreateBucketIfNotExists(
		queue,
		queueOrderBucketKey,
	)

	old := loadQueueItem(items, op.Item.ID())

	if op.Item.Revision != old.GetRevision() {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	// Ensure the envelope can not be modified.
	if op.Item.Revision > 0 {
		op.Item.Envelope = old.GetEnvelope()
	}

	new := saveQueueItem(items, op.Item)

	if new.GetNextAttemptAt() != old.GetNextAttemptAt() {
		if old != nil {
			removeQueueOrder(order, old)
		}

		saveQueueOrder(order, new)
	}

	return nil
}

// VisitRemoveQueueItem applies the changes in a "RemoveQueueItem" operation to
// the database.
func (c *committer) VisitRemoveQueueItem(
	ctx context.Context,
	op persistence.RemoveQueueItem,
) error {
	queue, ok := bboltx.TryBucket(
		c.root,
		queueBucketKey,
	)
	if !ok {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	items := bboltx.Bucket(
		queue,
		queueItemsBucketKey,
	)

	old := loadQueueItem(items, op.Item.ID())

	if op.Item.Revision != old.GetRevision() {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	order := bboltx.Bucket(
		queue,
		queueOrderBucketKey,
	)

	bboltx.Delete(items, []byte(op.Item.ID()))
	removeQueueOrder(order, old)

	return nil
}

// unmarshalQueueItem unmarshals a queuestore.Item from its binary
// representation.
func unmarshalQueueItem(data []byte) *queuestore.Item {
	var i pb.QueueItem
	bboltx.Must(proto.Unmarshal(data, &i))

	next, err := time.Parse(time.RFC3339Nano, i.NextAttemptAt)
	bboltx.Must(err)

	return &queuestore.Item{
		Revision:      i.Revision,
		FailureCount:  uint(i.FailureCount),
		NextAttemptAt: next,
		Envelope:      i.Envelope,
	}
}

// saveQueueItem saves a queue item to b.
func saveQueueItem(b *bbolt.Bucket, i queuestore.Item) *pb.QueueItem {
	v := &pb.QueueItem{
		Revision:      i.Revision + 1,
		FailureCount:  uint64(i.FailureCount),
		NextAttemptAt: i.NextAttemptAt.Format(time.RFC3339Nano),
		Envelope:      i.Envelope,
	}

	data, err := proto.Marshal(v)
	bboltx.Must(err)
	bboltx.Put(b, []byte(i.ID()), data)

	return v
}

// loadQueueItem loads a queue item with a specific message ID from b.
func loadQueueItem(b *bbolt.Bucket, id string) *pb.QueueItem {
	data := b.Get([]byte(id))
	if data == nil {
		return nil
	}

	var item pb.QueueItem
	err := proto.Unmarshal(data, &item)
	bboltx.Must(err)

	return &item
}

// saveQueueOrder adds a record for i to the order bucket.
func saveQueueOrder(order *bbolt.Bucket, i *pb.QueueItem) {
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
func removeQueueOrder(order *bbolt.Bucket, i *pb.QueueItem) {
	id := i.GetEnvelope().GetMetaData().GetMessageId()

	bboltx.Delete(
		bboltx.Bucket(
			order,
			[]byte(i.NextAttemptAt),
		),
		[]byte(id),
	)
}
