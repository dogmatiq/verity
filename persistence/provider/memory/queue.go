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
		t.uncommitted.queue = append(
			t.uncommitted.queue,
			&queue.Message{
				Revision: 1,
				Envelope: cloneEnvelope(env),
			},
		)
	}

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
