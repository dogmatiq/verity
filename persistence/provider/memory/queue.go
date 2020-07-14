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
		c.db.queue.insert(&op.Message)
	} else {
		c.db.queue.update(&op.Message)
	}
	return nil
}

// VisitRemoveQueueMessage applies the changes in a "RemoveQueueMessage"
// operation to the database.
func (c *committer) VisitRemoveQueueMessage(
	ctx context.Context,
	op persistence.RemoveQueueMessage,
) error {
	c.db.queue.remove(op.Message.ID())
	return nil
}

// queueDatabase contains message queue data.
type queueDatabase struct {
	order    []*persistence.QueueMessage
	messages map[string]*persistence.QueueMessage
	timeouts map[instanceKey]map[string]*persistence.QueueMessage
}

// insert inserts a new message into the queue.
func (db *queueDatabase) insert(m *persistence.QueueMessage) {
	m.Envelope = cloneEnvelope(m.Envelope)
	m.Revision++

	// Add the new message to the queue.
	if db.messages == nil {
		db.messages = map[string]*persistence.QueueMessage{}
	}
	db.messages[m.ID()] = m

	db.addToOrderIndex(m)
	db.addToTimeoutIndex(m)
}

// update updates an existing message.
func (db *queueDatabase) update(m *persistence.QueueMessage) {
	existing := db.messages[m.ID()]

	// Apply the changes to the database, ensuring that the envelope is never
	// changed.
	env := existing.Envelope
	*existing = *m
	existing.Envelope = env
	existing.Revision++

	// Queue messages are typically updated after an error, when their
	// next-attempt time is updated as per the backoff strategy, so in practice
	// we always need to sort.
	sort.Slice(
		db.order,
		func(i, j int) bool {
			return db.order[i].NextAttemptAt.Before(
				db.order[j].NextAttemptAt,
			)
		},
	)
}

// remove removes the message with the given id from the queue.
func (db *queueDatabase) remove(id string) {
	m := db.messages[id]
	delete(db.messages, id)

	db.removeFromOrderIndex(m)
	db.removeFromTimeoutIndex(m)
}

// removeTimeoutsByProcessInstance removes all timeout messages produced by a
// specific process instance.
func (db *queueDatabase) removeTimeoutsByProcessInstance(key instanceKey) {
	timeouts := db.timeouts[key]
	delete(db.timeouts, key)

	for id, m := range timeouts {
		delete(db.messages, id)
		db.removeFromOrderIndex(m)
	}
}

// addToOrderIndex adds m to the db.order index.
func (db *queueDatabase) addToOrderIndex(m *persistence.QueueMessage) {
	// Find the index in the ordered queue where the message belongs. It's the
	// index of the first message that has a NextAttemptedAt greater than
	// that of the new message.
	index := sort.Search(
		len(db.order),
		func(i int) bool {
			return m.NextAttemptAt.Before(
				db.order[i].NextAttemptAt,
			)
		},
	)

	// Expand the size of the queue.
	db.order = append(db.order, nil)

	// Shift lower-priority messages further back to make space for the new one.
	copy(db.order[index+1:], db.order[index:])

	// Insert the new message at it's sorted index.
	db.order[index] = m
}

// removeFromOrderIndex removes m from the db.order index.
func (db *queueDatabase) removeFromOrderIndex(m *persistence.QueueMessage) {
	// Use a binary search to find the index of the first message with the same
	// next-attempt time as the message we're trying to remove.
	i := sort.Search(
		len(db.order),
		func(i int) bool {
			return !db.order[i].NextAttemptAt.Before(
				m.NextAttemptAt,
			)
		},
	)

	// Then scan through the slice from that index to find the specific message.
	// It's probably the message at the start index, but more than one message
	// may have the same next-attempt time.
	for ; i < len(db.order); i++ {
		if db.order[i].ID() == m.ID() {
			db.order = append(
				db.order[:i],
				db.order[i+1:]...,
			)
			break
		}
	}
}

// addToTimeoutIndex adds m to the db.timeouts index, if it is a timeout
// message.
func (db *queueDatabase) addToTimeoutIndex(m *persistence.QueueMessage) {
	if m.Envelope.GetScheduledFor() == "" {
		return
	}

	key := instanceKey{
		m.Envelope.GetSourceHandler().GetKey(),
		m.Envelope.GetSourceInstanceId(),
	}

	if db.timeouts == nil {
		db.timeouts = map[instanceKey]map[string]*persistence.QueueMessage{}
	}

	timeouts := db.timeouts[key]

	if timeouts == nil {
		timeouts = map[string]*persistence.QueueMessage{}
		db.timeouts[key] = timeouts
	}

	timeouts[m.ID()] = m
}

// removeFromTimeoutIndex removes m from the db.timeouts index.
func (db *queueDatabase) removeFromTimeoutIndex(m *persistence.QueueMessage) {
	if m.Envelope.GetScheduledFor() == "" {
		return
	}

	key := instanceKey{
		m.Envelope.GetSourceHandler().GetKey(),
		m.Envelope.GetSourceInstanceId(),
	}

	timeouts := db.timeouts[key]
	delete(timeouts, m.ID())

	if len(timeouts) == 0 {
		delete(db.timeouts, key)
	}
}
