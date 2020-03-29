package memory

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
)

// EnqueueMessages adds messages to the application's message queue.
func (t *transaction) EnqueueMessages(
	ctx context.Context,
	envelopes []*envelopespec.Envelope,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	for _, env := range envelopes {
		var next time.Time

		if len(env.MetaData.ScheduledFor) != 0 {
			var err error
			next, err = time.Parse(time.RFC3339Nano, env.MetaData.ScheduledFor)
			if err != nil {
				return err
			}
		}

		t.uncommitted.queue = append(
			t.uncommitted.queue,
			&queue.Message{
				Revision:      1,
				NextAttemptAt: next,
				Envelope:      cloneEnvelope(env),
			},
		)
	}

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
// messages in memory.
type queueRepository struct {
	db *database
}

// LoadQueuedMessages loads the next n messages from the queue.
func (r *queueRepository) LoadQueuedMessages(
	ctx context.Context,
	n int,
) ([]*queue.Message, error) {
	if err := r.db.RLock(ctx); err != nil {
		return nil, err
	}
	defer r.db.RUnlock()

	max := len(r.db.queue)
	if n > max {
		n = max
	}

	result := make([]*queue.Message, n)

	for i := range result {
		result[i] = cloneQueuedMessage(
			r.db.queue[i],
		)
	}

	return result, nil
}
