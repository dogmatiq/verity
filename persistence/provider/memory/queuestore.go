package memory

import (
	"context"
	"sort"

	"github.com/dogmatiq/infix/internal/refactor251"
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

	if t.queue.stageSave(&t.ds.db.queue, i) {
		return nil
	}

	return refactor251.ErrConflict
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

	if t.queue.stageRemove(&t.ds.db.queue, i) {
		return nil
	}

	return refactor251.ErrConflict
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

	items := r.db.queue.view(n)
	result := make([]*queuestore.Item, len(items))

	for i, it := range items {
		result[i] = cloneQueueStoreItem(it)
	}

	return result, nil
}

// queueStoreChangeSet contains modifications to the queue store that have
// been performed within a transaction but not yet committed.
type queueStoreChangeSet struct {
	items map[string]*queuestore.Item
}

// stageSave adds a "SaveMessageToQueue" operation to the change-set.
//
// It returns false if there is an OCC conflict.
func (cs *queueStoreChangeSet) stageSave(
	db *queueStoreDatabase,
	i *queuestore.Item,
) bool {
	id := i.ID()

	// Get both the committed item, and the item as it appears with its changes
	// staged in this change-set.
	committed := db.items[id]
	staged, changed := cs.items[id]

	// The "effective" item is how the item appears to this transaction.
	effective := committed
	if changed {
		effective = staged
	}

	// Likewise, the "effective" revision is the revision as it appears
	// including the changes in this transaction.
	var effectiveRev uint64
	if effective != nil {
		// effective will be nil if the item is staged for removal, in which
		// case the effective revision is 0 again.
		effectiveRev = effective.Revision
	}

	// Enforce the optimistic concurrency control requirements.
	if i.Revision != effectiveRev {
		return false
	}

	if staged == nil {
		// If we don't have a staged value, either because this item has not
		// been modified within this change-set, or it has been marked for
		// removal, clone the item we're given and add it to the change-set.
		staged = cloneQueueStoreItem(i)

		if cs.items == nil {
			cs.items = map[string]*queuestore.Item{}
		}

		cs.items[id] = staged
	} else {
		// Otherwise, we already have our own clone, just mutate it to match the
		// new meta-data.
		copyQueueStoreItemMetaData(staged, i)
	}

	staged.Revision++

	return true
}

// stageRemove adds a "RemoveMessageFromQueue" operation to the change-set.
//
// It returns false if there is an OCC conflict.
func (cs *queueStoreChangeSet) stageRemove(
	db *queueStoreDatabase,
	i *queuestore.Item,
) bool {
	id := i.ID()

	// Get both the committed item, and the item as it appears with its changes
	// staged in this change-set.
	committed, exists := db.items[id]
	staged, changed := cs.items[id]

	// The "effective" item is how the item appears to this transaction.
	effective := committed
	if changed {
		effective = staged
	}

	// Enforce the optimistic concurrency control requirements.
	//
	// effective will be nil if the item is already staged for removal, in which
	// case from the perspective of this transaction it's trying to remove a
	// non-existent item.
	if effective == nil || i.Revision != effective.Revision {
		return false
	}

	if !exists {
		// The item is not actually committed to the database, but it has been
		// staged for saving, and is now being staged for removal. In this case
		// we simply remove it from the change-set.
		delete(cs.items, id)
		return true
	}

	// We mark the item for deletion by storing nil in the change-set.
	if cs.items == nil {
		cs.items = map[string]*queuestore.Item{}
	}
	cs.items[id] = nil

	return true
}

// view returns a slice of the items up to the given limit.
func (db *queueStoreDatabase) view(n int) []*queuestore.Item {
	max := len(db.order)
	if n > max {
		n = max
	}

	return db.order[:n]
}

// queueStoreDatabase contains data taht is committed to the queue store.
type queueStoreDatabase struct {
	order []*queuestore.Item
	items map[string]*queuestore.Item
}

// apply updates the database to include the changes in cs.
func (db *queueStoreDatabase) apply(cs *queueStoreChangeSet) {
	needsSort := false
	hasRemoves := false

	for id, staged := range cs.items {
		committed, exists := db.items[id]

		if !exists {
			// The staged item is not yet in the database. It is inserted at the
			// correct location, so no sorting is required.
			db.applyInsert(id, staged)
		} else if staged != nil {
			// The staged item *is* in the database, and it's non-nil meaning
			// that it's not a removal.
			if db.applyUpdate(id, committed, staged) {
				// The item's next-attempt time has changed, and so re-sorting
				// is required.
				needsSort = true
			}
		} else {
			// The staged item is a request for removal. Removals are applied by
			// sorting all removed items to the end of the ordered first and
			// then shrinking the slice.
			committed.Revision = 0
			needsSort = true
			hasRemoves = true
		}
	}

	if needsSort {
		sort.Slice(
			db.order,
			func(i, j int) bool {
				a := db.order[i]
				b := db.order[j]

				// If a message has a revision of 0 that means it's being
				// deleted, always sort it AFTER anything that's being kept.
				if a.Revision == 0 || b.Revision == 0 {
					return a.Revision > b.Revision
				}

				return a.NextAttemptAt.Before(b.NextAttemptAt)
			},
		)

		if hasRemoves {
			db.applyRemoves()
		}
	}
}

// applyInsert inserts a new item into the queue as part of a commit.
func (db *queueStoreDatabase) applyInsert(id string, staged *queuestore.Item) {
	// Add the staged item to the database.
	if db.items == nil {
		db.items = map[string]*queuestore.Item{}
	}
	db.items[id] = staged

	// Find the index in the ordered queue where the item belongs. It's the
	// index of the first item that has a NextAttemptedAt greater than
	// the staged item's.
	index := sort.Search(
		len(db.order),
		func(i int) bool {
			return staged.NextAttemptAt.Before(
				db.order[i].NextAttemptAt,
			)
		},
	)

	// Expand the size of the ordered queue.
	db.order = append(db.order, nil)

	// Shift lower-priority items further back to make space for the staged item.
	copy(db.order[index+1:], db.order[index:])

	// Insert the staged item at it's sorted index.
	db.order[index] = staged
}

// applyUpdate updates an existing item as part of a commit.
func (db *queueStoreDatabase) applyUpdate(id string, committed, staged *queuestore.Item) bool {
	// We need to sort if the next-attempt time has changed. We comapre this
	// before replacing the committed meta-data with the staged meta-data.
	needsSort := !staged.NextAttemptAt.Equal(committed.NextAttemptAt)

	// Apply the changes to the database.
	copyQueueStoreItemMetaData(committed, staged)

	return needsSort
}

// applyRemoves removes existing items as part of a commit.
//
// It assumes all items marked for deletion are at the tail of the slice.
func (db *queueStoreDatabase) applyRemoves() {
	i := len(db.order) - 1

	// Starting from the back of the slice, we zero the underlying array's
	// memory until we find an item that is not to be removed.
	for i >= 0 {
		it := db.order[i]

		if it.Revision > 0 {
			// We found a message that is to be retained, so there is nothing
			// more to remove.
			break
		}

		db.order[i] = nil // prevent memory leak
		delete(db.items, it.ID())

		i--
	}

	// Trim the ordered queue slice to the new length.
	db.order = db.order[:i+1]
}

// copyQueueStoreItemMetaData assigns meta-data from src to dest, retaining
// dest's original envelope.
func copyQueueStoreItemMetaData(dest, src *queuestore.Item) {
	env := dest.Envelope
	*dest = *src
	dest.Envelope = env
}

// clone returns a deep clone of an eventstore.Item.
func cloneQueueStoreItem(i *queuestore.Item) *queuestore.Item {
	clone := *i
	clone.Envelope = cloneEnvelope(clone.Envelope)
	return &clone
}
