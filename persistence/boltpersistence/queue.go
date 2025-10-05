package boltpersistence

import (
	"context"
	"time"

	"github.com/dogmatiq/verity/internal/x/bboltx"
	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/persistence/boltpersistence/internal/pb"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
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

	// queueTimeoutsBucketKey is the key for a child bucket that is used to
	// index queued timeout messages by the process instance that produced them.
	//
	// The keys are application-defined handler identity keys. The values are
	// are buckets indicating which timeout messages are pending.
	//
	// Within this further sub-bucket, the keys are the application-defined
	// message ID, and the values are always nil.
	queueTimeoutsBucketKey = []byte("timeouts")
)

// LoadQueueMessages loads the next n messages from the queue.
func (ds *dataStore) LoadQueueMessages(
	_ context.Context,
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

			order := bboltx.Bucket(
				tx,
				ds.appKey,
				queueBucketKey,
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
	_ context.Context,
	op persistence.SaveQueueMessage,
) error {
	oldMessage := loadQueueMessage(c.root, op.Message.ID())

	if op.Message.Revision != oldMessage.GetRevision() {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	// Ensure the envelope can not be modified.
	if op.Message.Revision > 0 {
		op.Message.Envelope = oldMessage.GetEnvelope()
	}

	newMessage := saveQueueMessage(c.root, op.Message)

	if newMessage.GetNextAttemptAt() != oldMessage.GetNextAttemptAt() {
		if oldMessage != nil {
			removeQueueOrder(c.root, oldMessage)
		}

		saveQueueOrder(c.root, newMessage)
	}

	addToTimeoutIndex(c.root, op.Message)

	return nil
}

// VisitRemoveQueueMessage applies the changes in a "RemoveQueueMessage"
// operation to the database.
func (c *committer) VisitRemoveQueueMessage(
	_ context.Context,
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
	removeFromTimeoutIndex(c.root, op.Message)

	return nil
}

// removeTimeoutsByProcessInstance removes all timeout messages produced by a
// specific process instance.
func (c *committer) removeTimeoutsByProcessInstance(hk, id string) {
	messageIDs, ok := bboltx.TryBucket(
		c.root,
		queueBucketKey,
		queueTimeoutsBucketKey,
		[]byte(hk),
		[]byte(id),
	)
	if !ok {
		return
	}

	cur := messageIDs.Cursor()
	for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
		old := loadQueueMessage(c.root, string(k))

		bboltx.DeletePath(
			c.root,
			queueBucketKey,
			queueMessagesBucketKey,
			k,
		)

		removeQueueOrder(c.root, old)
	}
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

// addToTimeoutIndex adds a recored for m to the timeout index.
func addToTimeoutIndex(root *bbolt.Bucket, m persistence.QueueMessage) {
	if m.Envelope.GetScheduledFor() != "" {
		bboltx.PutPath(
			root,
			nil,
			queueBucketKey,
			queueTimeoutsBucketKey,
			[]byte(m.Envelope.GetSourceHandler().GetKey()),
			[]byte(m.Envelope.GetSourceInstanceId()),
			[]byte(m.ID()),
		)
	}
}

// addToTimeoutIndex removes the record for m to the timeout index.
func removeFromTimeoutIndex(root *bbolt.Bucket, m persistence.QueueMessage) {
	if m.Envelope.GetScheduledFor() != "" {
		bboltx.DeletePath(
			root,
			queueBucketKey,
			queueTimeoutsBucketKey,
			[]byte(m.Envelope.GetSourceHandler().GetKey()),
			[]byte(m.Envelope.GetSourceInstanceId()),
			[]byte(m.ID()),
		)
	}
}
