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

// EnqueueMessages adds messages to the application's message queue.
func (t *transaction) EnqueueMessages(
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

	enqueueMessages(order, messages, envelopes)

	return nil
}

// DequeueMessage removes a message from the application's message queue.
//
// m.Revision must be the revision of the queued message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// remains on the queue and ok is false.
func (t *transaction) DequeueMessage(
	ctx context.Context,
	m *queue.Message,
) (ok bool, err error) {
	return false, errors.New("not implemented")
}

// UpdateQueuedMessage updates meta-data about a queued message.
//
// The following fields are updated:
//  - NextAttemptAt
//
// m.Revision must be the revision of the queued message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message is not
// updated and ok is false.
func (t *transaction) UpdateQueuedMessage(
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

// LoadQueuedMessages loads the next n messages from the queue.
func (r *queueRepository) LoadQueuedMessages(
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
					m := loadQueueMessage(messages, id)
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

// loadQueueMessage loads a message from the queue.
func loadQueueMessage(
	messages *bbolt.Bucket,
	id []byte,
) *queue.Message {
	data := messages.Get(id)

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

// enqueueMessages writes messages to the queue.
func enqueueMessages(
	order *bbolt.Bucket,
	messages *bbolt.Bucket,
	envelopes []*envelopespec.Envelope,
) {
	for _, env := range envelopes {
		id := []byte(env.MetaData.MessageId)

		if messages.Get(id) != nil {
			continue
		}

		penv := &pb.QueueMessage{
			Revision:      1,
			NextAttemptAt: env.MetaData.ScheduledFor,
			Envelope:      env,
		}

		if penv.NextAttemptAt == "" {
			penv.NextAttemptAt = env.MetaData.CreatedAt
		}

		data, err := proto.Marshal(penv)
		bboltx.Must(err)

		bboltx.Put(messages, id, data)
		bboltx.CreateBucketIfNotExists(
			order,
			[]byte(penv.NextAttemptAt),
		).Put(id, nil)
	}
}
