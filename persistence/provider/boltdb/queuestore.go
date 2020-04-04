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
// m.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// is not saved and ErrConflict is returned.
func (t *transaction) SaveMessageToQueue(
	ctx context.Context,
	m *queuestore.Message,
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

	old := loadQueueMessage(messages, m.ID())

	if uint64(m.Revision) != old.GetRevision() {
		return queuestore.ErrConflict
	}

	new, data := marshalQueueMessage(m, old)
	bboltx.Put(messages, []byte(m.ID()), data)

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
// m.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// remains on the queue and ErrConflict is returned.
func (t *transaction) RemoveMessageFromQueue(
	ctx context.Context,
	m *queuestore.Message,
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

	old := loadQueueMessage(messages, m.ID())

	if uint64(m.Revision) != old.GetRevision() {
		return queuestore.ErrConflict
	}

	order := bboltx.Bucket(
		t.actual,
		t.appKey,
		queueBucketKey,
		orderBucketKey,
	)

	bboltx.Delete(messages, []byte(m.ID()))
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
) (_ []*queuestore.Message, err error) {
	defer bboltx.Recover(&err)

	var result []*queuestore.Message

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
					m := unmarshalQueueMessage(data)
					result = append(result, m)

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

// unmarshalQueueMessage unmarshals a queue message from its binary
// representation.
func unmarshalQueueMessage(data []byte) *queuestore.Message {
	var m pb.QueueMessage
	bboltx.Must(proto.Unmarshal(data, &m))

	next, err := time.Parse(time.RFC3339Nano, m.NextAttemptAt)
	bboltx.Must(err)

	return &queuestore.Message{
		Revision:      queuestore.Revision(m.Revision),
		NextAttemptAt: next,
		Envelope:      m.Envelope,
	}
}

// marshalQueueMessage marshals a queue message to its binary representation.
func marshalQueueMessage(
	m *queuestore.Message,
	old *pb.QueueMessage,
) (*pb.QueueMessage, []byte) {
	new := &pb.QueueMessage{
		Revision:      uint64(m.Revision + 1),
		NextAttemptAt: m.NextAttemptAt.Format(time.RFC3339Nano),
		Envelope:      old.GetEnvelope(),
	}

	// Only use user-supplied envelope if there's no old one.
	// This satisfies the requirement that updates only modify meta-data.
	if new.Envelope == nil {
		new.Envelope = m.Envelope
	}

	data, err := proto.Marshal(new)
	bboltx.Must(err)

	return new, data
}

// loadQueueMessage loads the protobuf representation of a queue message.
func loadQueueMessage(messages *bbolt.Bucket, id string) *pb.QueueMessage {
	data := messages.Get([]byte(id))
	if data == nil {
		return nil
	}

	m := &pb.QueueMessage{}
	err := proto.Unmarshal(data, m)
	bboltx.Must(err)

	return m
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
