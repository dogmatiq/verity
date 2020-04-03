package memory

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// SaveMessageToQueue persists a message to the application's message queue.
//
// n indicates when the next attempt at handling the message is to be made.
func (t *transaction) SaveMessageToQueue(
	ctx context.Context,
	env *envelopespec.Envelope,
	n time.Time,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	id := env.GetMetaData().GetMessageId()

	if _, ok := t.ds.db.queue.uniq[id]; ok {
		return nil
	}

	if t.uncommitted.queue == nil {
		t.uncommitted.queue = map[string]*queuestore.Message{}
	}

	t.uncommitted.queue[id] = &queuestore.Message{
		Revision:      1,
		NextAttemptAt: n,
		Envelope:      cloneEnvelope(env),
	}

	return nil
}

// commitQueue commits staged queue items to the database.
func (t *transaction) commitQueue() {
	q := &t.ds.db.queue

	for id, m := range t.uncommitted.queue {
		// Add the message to the unique index.
		if q.uniq == nil {
			q.uniq = map[string]*queuestore.Message{}
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

// RemoveMessageFromQueue removes a specific message from the application's
// message queue.
//
// m.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// remains on the queue and ok is false.
func (t *transaction) RemoveMessageFromQueue(
	ctx context.Context,
	m *queuestore.Message,
) (ok bool, err error) {
	return false, errors.New("not implemented")
}

// queueStoreRepository is an implementation of queuestore.Repository that
// stores queued messages in memory.
type queueStoreRepository struct {
	db *database
}

// LoadQueueMessages loads the next n messages from the queue.
func (r *queueStoreRepository) LoadQueueMessages(
	ctx context.Context,
	n int,
) ([]*queuestore.Message, error) {
	if err := r.db.RLock(ctx); err != nil {
		return nil, err
	}
	defer r.db.RUnlock()

	max := len(r.db.queue.order)
	if n > max {
		n = max
	}

	result := make([]*queuestore.Message, n)

	for i, m := range r.db.queue.order[:n] {
		result[i] = cloneQueueMessage(m)
	}

	return result, nil
}
