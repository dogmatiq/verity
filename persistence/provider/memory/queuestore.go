package memory

import (
	"context"
	"sort"

	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// SaveMessageToQueue persists a message to the application's message queue.
//
// If the message is already on the queue its meta-data is updated.
//
// i.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// is not saved and ErrConflict is returned.
func (t *transaction) SaveMessageToQueue(
	ctx context.Context,
	i *queuestore.Item,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	id := i.ID()
	committed := t.ds.db.queue.uniq[id]
	uncommitted, changed := t.uncommitted.queue[id]

	effective := committed

	if changed {
		effective = uncommitted
	}

	var rev queuestore.Revision
	if effective != nil {
		rev = effective.Revision
	}

	if i.Revision != rev {
		return queuestore.ErrConflict
	}

	if uncommitted == nil {
		uncommitted = cloneQueueStoreItem(i)
	} else {
		assignMetaData(uncommitted, i)
	}

	uncommitted.Revision++

	if t.uncommitted.queue == nil {
		t.uncommitted.queue = map[string]*queuestore.Item{}
	}
	t.uncommitted.queue[id] = uncommitted

	return nil
}

// RemoveMessageFromQueue removes a specific message from the application's
// message queue.
//
// i.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// remains on the queue and ErrConflict is returned.
func (t *transaction) RemoveMessageFromQueue(
	ctx context.Context,
	i *queuestore.Item,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	id := i.ID()
	committed, exists := t.ds.db.queue.uniq[id]
	uncommitted, changed := t.uncommitted.queue[id]

	effective := committed
	if changed {
		effective = uncommitted
	}

	if effective == nil || i.Revision != effective.Revision {
		return queuestore.ErrConflict
	}

	if !exists {
		// The message has been saved in this transaction, is now being deleted,
		// and never existed before this transaction.
		delete(t.uncommitted.queue, id)
		return nil
	}

	if t.uncommitted.queue == nil {
		t.uncommitted.queue = map[string]*queuestore.Item{}
	}
	t.uncommitted.queue[id] = nil

	return nil
}

// commitQueue commits staged queue items to the database.
func (t *transaction) commitQueue() {
	q := &t.ds.db.queue

	needsSort := false
	hasDeletes := false

	for id, uncommitted := range t.uncommitted.queue {
		committed, exists := q.uniq[id]

		if !exists {
			t.commitQueueInsert(id, uncommitted)
		} else if uncommitted != nil {
			if t.commitQueueUpdate(id, committed, uncommitted) {
				needsSort = true
			}
		} else {
			committed.Revision = 0 // mark for deletion
			needsSort = true
			hasDeletes = true
		}
	}

	if needsSort {
		sort.Slice(
			q.order,
			func(i, j int) bool {
				a := q.order[i]
				b := q.order[j]

				// If a message has a revision of 0 that means it's being
				// deleted, always sort it AFTER anything that's being kept.
				if a.Revision == 0 || b.Revision == 0 {
					return a.Revision > b.Revision
				}

				return a.NextAttemptAt.Before(b.NextAttemptAt)
			},
		)

		if hasDeletes {
			t.commitQueueRemovals()
		}
	}
}

// commitQueueInsert inserts a new message into the queue as part of a commit.
func (t *transaction) commitQueueInsert(
	id string,
	uncommitted *queuestore.Item,
) {
	q := &t.ds.db.queue

	// Add the message to the unique index.
	if q.uniq == nil {
		q.uniq = map[string]*queuestore.Item{}
	}
	q.uniq[id] = uncommitted

	// Find the index where we'll insert our message. It's the index of the
	// first message that has a NextAttemptedAt greater than uncommitted's.
	index := sort.Search(
		len(q.order),
		func(i int) bool {
			return uncommitted.NextAttemptAt.Before(
				q.order[i].NextAttemptAt,
			)
		},
	)

	// Expand the size of the queue.
	q.order = append(q.order, nil)

	// Shift messages further back to make space for uncommitted.
	copy(q.order[index+1:], q.order[index:])

	// Insert uncommitted at the index.
	q.order[index] = uncommitted
}

// commitQueueUpdate updates an existing message as part of a commit.
func (t *transaction) commitQueueUpdate(
	id string,
	committed *queuestore.Item,
	uncommitted *queuestore.Item,
) bool {
	needsSort := !uncommitted.NextAttemptAt.Equal(committed.NextAttemptAt)
	assignMetaData(committed, uncommitted)
	return needsSort
}

// commitQueueRemovals removes existing messages as part of a commit.
func (t *transaction) commitQueueRemovals() {
	q := &t.ds.db.queue

	i := len(q.order) - 1

	for i >= 0 {
		it := q.order[i]

		if it.Revision > 0 {
			// All deleted messages have a revision of 0 and have been sorted to
			// the end, so once we find a real revision, we're done.
			break
		}

		q.order[i] = nil // prevent memory leak
		delete(q.uniq, it.ID())

		i--
	}

	// Trim the slice to the new length.
	q.order = q.order[:i+1]
}

// assignMetaData assigns meta-data from src to dest, retaining dest's original
// envelope.
func assignMetaData(dest, src *queuestore.Item) {
	env := dest.Envelope
	*dest = *src
	dest.Envelope = env
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
) ([]*queuestore.Item, error) {
	if err := r.db.RLock(ctx); err != nil {
		return nil, err
	}
	defer r.db.RUnlock()

	max := len(r.db.queue.order)
	if n > max {
		n = max
	}

	result := make([]*queuestore.Item, n)

	for i, it := range r.db.queue.order[:n] {
		result[i] = cloneQueueStoreItem(it)
	}

	return result, nil
}
