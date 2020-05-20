package memory

import (
	"context"
	"sort"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// LoadQueueMessages loads the next n messages from the queue.
func (ds *dataStore) LoadQueueMessages(
	ctx context.Context,
	n int,
) ([]*queuestore.Item, error) {
	ds.db.mutex.RLock()
	defer ds.db.mutex.RUnlock()

	max := len(ds.db.queue.order)
	if n > max {
		n = max
	}

	items := make([]*queuestore.Item, n)
	for i, it := range ds.db.queue.order[:n] {
		items[i] = cloneQueueItem(it)
	}

	return items, nil
}

// queueDatabase contains data taht is committed to the queue store.
type queueDatabase struct {
	order []*queuestore.Item
	items map[string]*queuestore.Item
}

// VisitSaveQueueItem returns an error if a "SaveQueueItem" operation can not be
// applied to the database.
func (v *validator) VisitSaveQueueItem(
	_ context.Context,
	op persistence.SaveQueueItem,
) error {
	if x, ok := v.db.queue.items[op.Item.ID()]; ok {
		if op.Item.Revision == x.Revision {
			return nil
		}
	} else if op.Item.Revision == 0 {
		return nil
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitRemoveQueueItem returns an error if a "RemoveQueueItem" operation can
// not be applied to the database.
func (v *validator) VisitRemoveQueueItem(
	ctx context.Context,
	op persistence.RemoveQueueItem,
) error {
	if x, ok := v.db.queue.items[op.Item.ID()]; ok {
		if op.Item.Revision == x.Revision {
			return nil
		}
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitSaveQueueItem applies the changes in a "SaveQueueItem" operation to the
// database.
func (c *committer) VisitSaveQueueItem(
	ctx context.Context,
	op persistence.SaveQueueItem,
) error {
	if op.Item.Revision == 0 {
		c.insertQueueItem(op)
	} else {
		c.updateQueueItem(op)
	}
	return nil
}

// insertQueueItem inserts a new item into the queue.
func (c *committer) insertQueueItem(op persistence.SaveQueueItem) {
	item := cloneQueueItem(&op.Item)
	item.Revision++

	// Add the new item to the queue.
	if c.db.queue.items == nil {
		c.db.queue.items = map[string]*queuestore.Item{}
	}
	c.db.queue.items[item.ID()] = item

	// Find the index in the ordered queue where the item belongs. It's the
	// index of the first item that has a NextAttemptedAt greater than
	// the new item's.
	index := sort.Search(
		len(c.db.queue.order),
		func(i int) bool {
			return op.Item.NextAttemptAt.Before(
				c.db.queue.order[i].NextAttemptAt,
			)
		},
	)

	// Expand the size of the queue.
	c.db.queue.order = append(c.db.queue.order, nil)

	// Shift lower-priority items further back to make space for the new item.
	copy(c.db.queue.order[index+1:], c.db.queue.order[index:])

	// Insert the new item at it's sorted index.
	c.db.queue.order[index] = item
}

// updateQueueItem updates an existing item.
func (c *committer) updateQueueItem(op persistence.SaveQueueItem) {
	existing := c.db.queue.items[op.Item.ID()]

	// Apply the changes to the database.
	copyQueueItemMetaData(existing, &op.Item)
	existing.Revision++

	// Queue items are typically updated after an error, when their next-attempt
	// time is updated as per the backoff strategy, so in practice we always
	// need to sort.
	sort.Slice(
		c.db.queue.order,
		func(i, j int) bool {
			return c.db.queue.order[i].NextAttemptAt.Before(
				c.db.queue.order[j].NextAttemptAt,
			)
		},
	)
}

// VisitRemoveQueueItem applies the changes in a "RemoveQueueItem" operation to
// the database.
func (c *committer) VisitRemoveQueueItem(
	ctx context.Context,
	op persistence.RemoveQueueItem,
) error {
	existing := c.db.queue.items[op.Item.ID()]
	delete(c.db.queue.items, op.Item.ID())

	// Use a binary search to find the index of the first item with the same
	// next-attempt time as the item we're trying to remove.
	start := sort.Search(
		len(c.db.queue.order),
		func(i int) bool {
			return !c.db.queue.order[i].NextAttemptAt.Before(
				existing.NextAttemptAt,
			)
		},
	)

	// Then scan through the slice from that index to find the specific item.
	// It's probably the item at the start index, but more than one item may
	// have the same next-attempt time.
	for i, item := range c.db.queue.order[start:] {
		if item.ID() == op.Item.ID() {
			c.db.queue.order = append(
				c.db.queue.order[:i],
				c.db.queue.order[i+1:]...,
			)
			break
		}
	}

	return nil
}

// copyQueueItemMetaData assigns meta-data from src to dest, retaining dest's
// original envelope.
func copyQueueItemMetaData(dest, src *queuestore.Item) {
	env := dest.Envelope
	*dest = *src
	dest.Envelope = env
}

// cloneQueueItem returns a deep clone of an eventstore.Item.
func cloneQueueItem(i *queuestore.Item) *queuestore.Item {
	clone := *i
	clone.Envelope = cloneEnvelope(clone.Envelope)
	return &clone
}
