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
			messages, ok := bboltx.TryBucket(
				tx,
				ds.appKey,
				queueBucketKey,
				queueMessagesBucketKey,
			)
			if !ok {
				return
			}

			order, ok := bboltx.TryBucket(
				tx,
				ds.appKey,
				queueBucketKey,
				queueOrderBucketKey,
			)
			if !ok {
				return
			}

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
	old := loadQueueMessage(c.root, op.Message.ID())

	if op.Message.Revision != old.GetRevision() {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	// Ensure the envelope can not be modified.
	if op.Message.Revision > 0 {
		op.Message.Envelope = old.GetEnvelope()
	}

	new := saveQueueMessage(c.root, op.Message)

	if new.GetNextAttemptAt() != old.GetNextAttemptAt() {
		if old != nil {
			removeQueueOrder(c.root, old)
		}

		saveQueueOrder(c.root, new)
	}

	return nil
}

// VisitRemoveQueueMessage applies the changes in a "RemoveQueueMessage"
// operation to the database.
func (c *committer) VisitRemoveQueueMessage(
	ctx context.Context,
	op persistence.RemoveQueueMessage,
) error {
	old := loadQueueMessage(c.root, op.Message.ID())

	if old == nil || op.Message.Revision != old.GetRevision() {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	bboltx.DeletePath(
		c.root,
		queueBucketKey,
		queueMessagesBucketKey,
		[]byte(op.Message.ID()),
	)
	removeQueueOrder(c.root, old)

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
func saveQueueMessage(root *bbolt.Bucket, m persistence.QueueMessage) *pb.QueueMessage {
	v := &pb.QueueMessage{
		Revision:      m.Revision + 1,
		FailureCount:  uint64(m.FailureCount),
		NextAttemptAt: m.NextAttemptAt.Format(time.RFC3339Nano),
		Envelope:      m.Envelope,
	}

	data, err := proto.Marshal(v)
	bboltx.Must(err)

	bboltx.PutPath(
		root,
		data,
		queueBucketKey,
		queueMessagesBucketKey,
		[]byte(m.ID()),
	)

	return v
}

// loadQueueMessage loads a queue message with a specific message ID from b.
func loadQueueMessage(root *bbolt.Bucket, id string) *pb.QueueMessage {
	data := bboltx.GetPath(
		root,
		queueBucketKey,
		queueMessagesBucketKey,
		[]byte(id),
	)
	if data == nil {
		return nil
	}

	var m pb.QueueMessage
	err := proto.Unmarshal(data, &m)
	bboltx.Must(err)

	return &m
}

// saveQueueOrder adds a record for m to the order bucket.
func saveQueueOrder(root *bbolt.Bucket, m *pb.QueueMessage) {
	bboltx.PutPath(
		root,
		nil,
		queueBucketKey,
		queueOrderBucketKey,
		[]byte(m.GetNextAttemptAt()),
		[]byte(m.GetEnvelope().GetMessageId()),
	)
}

// removeQueueOrder removes the record for m from the order bucket.
func removeQueueOrder(root *bbolt.Bucket, m *pb.QueueMessage) {
	bboltx.DeletePath(
		root,
		queueBucketKey,
		queueOrderBucketKey,
		[]byte(m.GetNextAttemptAt()),
		[]byte(m.GetEnvelope().GetMessageId()),
	)
}
