package boltdb

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/boltdb/internal/pb"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/bbolt"
)

var (
	// queueBucketKey is the key for the root bucket for the message queue.
	queueBucketKey = []byte("queue")

	// queueMessagesBucketKey is the key for a child bucket that contains each
	// queued message.
	//
	// The keys are the application-defined message ID. The values are
	// pb.QueueMessage values marshaled using protocol buffers.
	queueMessagesBucketKey = []byte("messages")

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
) (_ []persistence.QueueMessage, err error) {
	defer bboltx.Recover(&err)

	var result []persistence.QueueMessage

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

			messages := bboltx.Bucket(
				queue,
				queueMessagesBucketKey,
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
					result = append(
						result,
						unmarshalQueueMessage(
							messages.Get(id),
						),
					)

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

// VisitSaveQueueMessage applies the changes in a "SaveQueueMessage" operation
// to the database.
func (c *committer) VisitSaveQueueMessage(
	ctx context.Context,
	op persistence.SaveQueueMessage,
) error {
	queue := bboltx.CreateBucketIfNotExists(
		c.root,
		queueBucketKey,
	)

	messages := bboltx.CreateBucketIfNotExists(
		queue,
		queueMessagesBucketKey,
	)

	order := bboltx.CreateBucketIfNotExists(
		queue,
		queueOrderBucketKey,
	)

	old := loadQueueMessage(messages, op.Message.ID())

	if op.Message.Revision != old.GetRevision() {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	// Ensure the envelope can not be modified.
	if op.Message.Revision > 0 {
		op.Message.Envelope = old.GetEnvelope()
	}

	new := saveQueueMessage(messages, op.Message)

	if new.GetNextAttemptAt() != old.GetNextAttemptAt() {
		if old != nil {
			removeQueueOrder(order, old)
		}

		saveQueueOrder(order, new)
	}

	return nil
}

// VisitRemoveQueueMessage applies the changes in a "RemoveQueueMessage"
// operation to the database.
func (c *committer) VisitRemoveQueueMessage(
	ctx context.Context,
	op persistence.RemoveQueueMessage,
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

	messages := bboltx.Bucket(
		queue,
		queueMessagesBucketKey,
	)

	old := loadQueueMessage(messages, op.Message.ID())

	if op.Message.Revision != old.GetRevision() {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	order := bboltx.Bucket(
		queue,
		queueOrderBucketKey,
	)

	bboltx.Delete(messages, []byte(op.Message.ID()))
	removeQueueOrder(order, old)

	return nil
}

// unmarshalQueueMessage unmarshals a persistence.QueueMessage from its binary
// representation.
func unmarshalQueueMessage(data []byte) persistence.QueueMessage {
	var m pb.QueueMessage
	bboltx.Must(proto.Unmarshal(data, &m))

	next, err := time.Parse(time.RFC3339Nano, m.NextAttemptAt)
	bboltx.Must(err)

	return persistence.QueueMessage{
		Revision:      m.Revision,
		FailureCount:  uint(m.FailureCount),
		NextAttemptAt: next,
		Envelope:      m.Envelope,
	}
}

// saveQueueMessage saves a persistence.QueueMessage to b.
func saveQueueMessage(b *bbolt.Bucket, m persistence.QueueMessage) *pb.QueueMessage {
	v := &pb.QueueMessage{
		Revision:      m.Revision + 1,
		FailureCount:  uint64(m.FailureCount),
		NextAttemptAt: m.NextAttemptAt.Format(time.RFC3339Nano),
		Envelope:      m.Envelope,
	}

	data, err := proto.Marshal(v)
	bboltx.Must(err)
	bboltx.Put(b, []byte(m.ID()), data)

	return v
}

// loadQueueMessage loads a queue message with a specific message ID from b.
func loadQueueMessage(b *bbolt.Bucket, id string) *pb.QueueMessage {
	data := b.Get([]byte(id))
	if data == nil {
		return nil
	}

	var m pb.QueueMessage
	err := proto.Unmarshal(data, &m)
	bboltx.Must(err)

	return &m
}

// saveQueueOrder adds a record for m to the order bucket.
func saveQueueOrder(order *bbolt.Bucket, m *pb.QueueMessage) {
	id := m.GetEnvelope().GetMetaData().GetMessageId()

	bboltx.Put(
		bboltx.CreateBucketIfNotExists(
			order,
			[]byte(m.NextAttemptAt),
		),
		[]byte(id),
		nil,
	)
}

// removeQueueOrder removes the record for m from the order bucket.
func removeQueueOrder(order *bbolt.Bucket, m *pb.QueueMessage) {
	id := m.GetEnvelope().GetMetaData().GetMessageId()

	bboltx.Delete(
		bboltx.Bucket(
			order,
			[]byte(m.NextAttemptAt),
		),
		[]byte(id),
	)
}
