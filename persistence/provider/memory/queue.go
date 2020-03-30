package memory

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
)

// AddMessagesToQueue adds messages to the application's message queue.
func (t *transaction) AddMessagesToQueue(
	ctx context.Context,
	envelopes []*envelopespec.Envelope,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	for _, env := range envelopes {

		id := env.GetMetaData().GetMessageId()

		if _, ok := t.ds.db.queue.uniq[id]; ok {
			continue
		}

		data := env.MetaData.ScheduledFor
		if data == "" {
			data = env.MetaData.CreatedAt
		}

		next, err := time.Parse(time.RFC3339Nano, data)
		if err != nil {
			return err
		}

		if t.uncommitted.queue == nil {
			t.uncommitted.queue = map[string]*queue.Message{}
		}

		t.uncommitted.queue[id] = &queue.Message{
			Revision:      1,
			NextAttemptAt: next,
			Envelope:      cloneEnvelope(env),
		}
	}

	return nil
}

// commitQueue commits staged queue items to the database.
func (t *transaction) commitQueue() {
	q := &t.ds.db.queue

	for id, m := range t.uncommitted.queue {
		// Add the message to the unique index.
		if q.uniq == nil {
			q.uniq = map[string]*queue.Message{}
		}
		q.uniq[id] = m

		// Find the index where we'll insert our event. It's the index of the
		// first message that has a NextAttemptedAt greater than m's.
		index := sort.Search(
			len(q.order),
			func(i int) bool {
				return m.NextAttemptAt.Before(
					q.order[i].NextAttemptAt,
				)
			},
		)

		// Expand the size of the queue.
		q.order = append(q.order, nil)

		// Shift messages further back to make space for m.
		copy(q.order[index+1:], q.order[index:])

		// Insert m at the index.
		q.order[index] = m
	}
}

// DequeueMessage removes a message from the application's message queue.
//
// m.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// remains on the queue and ok is false.
func (t *transaction) DequeueMessage(
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
// messages in memory.
type queueRepository struct {
	db *database
}

// LoadQueueMessages loads the next n messages from the queue.
func (r *queueRepository) LoadQueueMessages(
	ctx context.Context,
	n int,
) ([]*queue.Message, error) {
	if err := r.db.RLock(ctx); err != nil {
		return nil, err
	}
	defer r.db.RUnlock()

	max := len(r.db.queue.order)
	if n > max {
		n = max
	}

	result := make([]*queue.Message, n)

	for i, m := range r.db.queue.order[:n] {
		result[i] = cloneQueueMessage(m)
	}

	return result, nil
}
