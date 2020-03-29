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

	messages := bboltx.CreateBucketIfNotExists(
		t.actual,
		t.appKey,
		queueBucketKey,
		messagesBucketKey,
	)

	enqueueMessages(messages, envelopes)

	return nil
}

// DequeueMessage removes a message from the application's message queue.
//
// m.Revision must be the revision of the queued message as currently
// persisted, otherwise an optimistic concurrency conflict has occured, the
// message remains on the queue and ok is false.
func (t *transaction) DequeueMessage(
	ctx context.Context,
	m *queue.Message,
) (ok bool, err error) {
	return false, errors.New("not implemented")
}

// DelayQueuedMessage returns defers the next attempt of a queued message
// after a failure.
//
// n is the time at which the next attempt at handling the message occurs.
//
// m.Revision must be the revision of the queued message as currently
// persisted, otherwise an optimistic concurrency conflict has occured, the
// message is not delayed and ok is false.
func (t *transaction) DelayQueuedMessage(
	ctx context.Context,
	m *queue.Message,
	n time.Time,
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
			messages, exists := bboltx.TryBucket(
				tx,
				r.appKey,
				queueBucketKey,
				messagesBucketKey,
			)

			if exists {
				c := messages.Cursor()

				k, v := c.First()

				for k != nil && len(result) < n {
					m := unmarshalQueueMessage(v)
					result = append(result, m)

					k, v = c.Next()
				}
			}
		},
	)

	return result, nil
}

var (
	queueBucketKey    = []byte("queue")
	messagesBucketKey = []byte("messages")
)

// unmarshalQueueMessage unmarshals a queue message from its protobuf
// representation.
func unmarshalQueueMessage(data []byte) *queue.Message {
	var m pb.QueueMessage
	bboltx.Must(proto.Unmarshal(data, &m))

	return &queue.Message{
		Revision: queue.Revision(m.Revision),
		Envelope: m.Envelope,
	}
}

// enqueueMessages writes messages to the queue.
func enqueueMessages(
	b *bbolt.Bucket,
	envelopes []*envelopespec.Envelope,
) {
	for _, env := range envelopes {
		penv := &pb.QueueMessage{
			Revision: 1,
			Envelope: env,
		}

		k := []byte(env.MetaData.MessageId)
		v, err := proto.Marshal(penv)
		bboltx.Must(err)

		bboltx.Put(b, k, v)
	}
}
