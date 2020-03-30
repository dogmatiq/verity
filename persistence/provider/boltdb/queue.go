package boltdb

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence/provider/boltdb/internal/pb"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/bbolt"
)

// AddMessagesToQueue adds messages to the application's message queue.
func (t *transaction) AddMessagesToQueue(
	ctx context.Context,
	envelopes []*envelopespec.Envelope,
) (err error) {
	defer bboltx.Recover(&err)

	if err := t.begin(ctx); err != nil {
		return err
	}

	order := bboltx.CreateBucketIfNotExists(
		t.actual,
		t.appKey,
		queueBucketKey,
		orderBucketKey,
	)

	messages := bboltx.CreateBucketIfNotExists(
		t.actual,
		t.appKey,
		queueBucketKey,
		messagesBucketKey,
	)

	for _, env := range envelopes {
		id := []byte(env.MetaData.MessageId)

		if messages.Get(id) != nil {
			continue
		}

		m, data := marshalEnvelopeAsQueueMessage(env)

		bboltx.Put(messages, id, data)
		bboltx.CreateBucketIfNotExists(
			order,
			[]byte(m.NextAttemptAt),
		).Put(id, nil)
	}

	return nil
}

// RemoveMessageFromQueue removes a specific message from the application's
// message queue.
//
// m.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// remains on the queue and ok is false.
func (t *transaction) RemoveMessageFromQueue(
	ctx context.Context,
	m *queue.Message,
) (ok bool, err error) {
	return false, errors.New("not implemented")
}

// UpdateQueueMessage updates meta-data about a message on the queue.
//
// The following fields are updated:
//  - NextAttemptAt
//
// m.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message is not
// updated and ok is false.
func (t *transaction) UpdateQueueMessage(
	ctx context.Context,
	m *queue.Message,
) (ok bool, err error) {
	return false, errors.New("not implemented")
}

// queueRepository is an implementation of queue.Repository that stores queued
// messages in a BoltDB database.
type queueRepository struct {
	db     *database
	appKey []byte
}

// LoadQueueMessages loads the next n messages from the queue.
func (r *queueRepository) LoadQueueMessages(
	ctx context.Context,
	n int,
) (_ []*queue.Message, err error) {
	defer bboltx.Recover(&err)

	var result []*queue.Message

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
func unmarshalQueueMessage(data []byte) *queue.Message {
	var m pb.QueueMessage
	bboltx.Must(proto.Unmarshal(data, &m))

	next, err := time.Parse(time.RFC3339Nano, m.NextAttemptAt)
	bboltx.Must(err)

	return &queue.Message{
		Revision:      queue.Revision(m.Revision),
		NextAttemptAt: next,
		Envelope:      m.Envelope,
	}
}

// marshalEnvelopeAsQueueMessage marshals an envelope to its binary
// representation as a queue.Message.
func marshalEnvelopeAsQueueMessage(
	env *envelopespec.Envelope,
) (*pb.QueueMessage, []byte) {
	m := &pb.QueueMessage{
		Revision:      1,
		NextAttemptAt: env.MetaData.ScheduledFor,
		Envelope:      env,
	}

	if m.NextAttemptAt == "" {
		m.NextAttemptAt = env.MetaData.CreatedAt
	}

	data, err := proto.Marshal(m)
	bboltx.Must(err)

	return m, data
}
