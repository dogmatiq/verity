package memory

import (
	"context"
	"sort"

	"github.com/dogmatiq/infix/persistence"
)

// LoadQueueMessages loads the next n messages from the queue.
func (ds *dataStore) LoadQueueMessages(
	ctx context.Context,
	n int,
) ([]persistence.QueueMessage, error) {
	ds.db.mutex.RLock()
	defer ds.db.mutex.RUnlock()

	max := len(ds.db.queue.order)
	if n > max {
		n = max
	}

	messages := make([]persistence.QueueMessage, n)
	for i, m := range ds.db.queue.order[:n] {
		// Clone the envelope on the way out so inadvertent manipulation of
		// the envelope by the caller does not affect the data in the
		// database.
		clone := *m
		clone.Envelope = cloneEnvelope(m.Envelope)

		messages[i] = clone
	}

	return messages, nil
}

// queueDatabase contains message queue data.
type queueDatabase struct {
	order    []*persistence.QueueMessage
	messages map[string]*persistence.QueueMessage
}

// VisitSaveQueueMessage returns an error if a "SaveQueueMessage" operation can
// not be applied to the database.
func (v *validator) VisitSaveQueueMessage(
	_ context.Context,
	op persistence.SaveQueueMessage,
) error {
	if x, ok := v.db.queue.messages[op.Message.ID()]; ok {
		if op.Message.Revision == x.Revision {
			return nil
		}
	} else if op.Message.Revision == 0 {
		return nil
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitRemoveQueueMessage returns an error if a "RemoveQueueMessage" operation
// can not be applied to the database.
func (v *validator) VisitRemoveQueueMessage(
	ctx context.Context,
	op persistence.RemoveQueueMessage,
) error {
	if x, ok := v.db.queue.messages[op.Message.ID()]; ok {
		if op.Message.Revision == x.Revision {
			return nil
		}
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitSaveQueueMessage applies the changes in a "SaveQueueMessage" operation
// to the database.
func (c *committer) VisitSaveQueueMessage(
	ctx context.Context,
	op persistence.SaveQueueMessage,
) error {
	if op.Message.Revision == 0 {
		c.insertQueueMessage(op)
	} else {
		c.updateQueueMessage(op)
	}
	return nil
}

// insertQueueMessage inserts a new message into the queue.
func (c *committer) insertQueueMessage(op persistence.SaveQueueMessage) {
	m := op.Message
	m.Envelope = cloneEnvelope(m.Envelope)
	m.Revision++

	// Add the new message to the queue.
	if c.db.queue.messages == nil {
		c.db.queue.messages = map[string]*persistence.QueueMessage{}
	}
	c.db.queue.messages[m.ID()] = &m

	// Find the index in the ordered queue where the message belongs. It's the
	// index of the first message that has a NextAttemptedAt greater than
	// that of the new message.
	index := sort.Search(
		len(c.db.queue.order),
		func(i int) bool {
			return op.Message.NextAttemptAt.Before(
				c.db.queue.order[i].NextAttemptAt,
			)
		},
	)

	// Expand the size of the queue.
	c.db.queue.order = append(c.db.queue.order, nil)

	// Shift lower-priority messages further back to make space for the new one.
	copy(c.db.queue.order[index+1:], c.db.queue.order[index:])

	// Insert the new message at it's sorted index.
	c.db.queue.order[index] = &m
}

// updateQueueMessage updates an existing message.
func (c *committer) updateQueueMessage(op persistence.SaveQueueMessage) {
	existing := c.db.queue.messages[op.Message.ID()]

	// Apply the changes to the database, ensuring that the envelope is never
	// changed.
	env := existing.Envelope
	*existing = op.Message
	existing.Envelope = env
	existing.Revision++

	// Queue messages are typically updated after an error, when their
	// next-attempt time is updated as per the backoff strategy, so in practice
	// we always need to sort.
	sort.Slice(
		c.db.queue.order,
		func(i, j int) bool {
			return c.db.queue.order[i].NextAttemptAt.Before(
				c.db.queue.order[j].NextAttemptAt,
			)
		},
	)
}

// VisitRemoveQueueMessage applies the changes in a "RemoveQueueMessage"
// operation to the database.
func (c *committer) VisitRemoveQueueMessage(
	ctx context.Context,
	op persistence.RemoveQueueMessage,
) error {
	existing := c.db.queue.messages[op.Message.ID()]
	delete(c.db.queue.messages, op.Message.ID())

	// Use a binary search to find the index of the first message with the same
	// next-attempt time as the message we're trying to remove.
	start := sort.Search(
		len(c.db.queue.order),
		func(i int) bool {
			return !c.db.queue.order[i].NextAttemptAt.Before(
				existing.NextAttemptAt,
			)
		},
	)

	// Then scan through the slice from that index to find the specific message.
	// It's probably the message at the start index, but more than one message
	// may have the same next-attempt time.
	for i, m := range c.db.queue.order[start:] {
		if m.ID() == op.Message.ID() {
			c.db.queue.order = append(
				c.db.queue.order[:i],
				c.db.queue.order[i+1:]...,
			)
			break
		}
	}

	return nil
}
