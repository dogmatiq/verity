package persistence

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// QueueMessage is a message persisted in the message queue.
type QueueMessage struct {
	Revision      uint64
	FailureCount  uint
	NextAttemptAt time.Time
	Envelope      *envelopespec.Envelope
}

// ID returns the ID of the message.
func (m QueueMessage) ID() string {
	return m.Envelope.MetaData.MessageId
}

// QueueRepository is an interface for reading queued messages.
type QueueRepository interface {
	// LoadQueueMessages loads the next n messages from the queue.
	LoadQueueMessages(ctx context.Context, n int) ([]QueueMessage, error)
}

// SaveQueueMessage is an Operation that saves a message to the queue, or
// updates a message that is already on the queue.
type SaveQueueMessage struct {
	// Message is the message to persist to the queue.
	//
	// The message's revision field must be the revision of the message as
	// currently persisted, otherwise an optimistic concurrency conflict occurs
	// and the entire batch of operations is rejected.
	Message QueueMessage
}

// AcceptVisitor calls v.VisitSaveQueueMessage().
func (op SaveQueueMessage) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitSaveQueueMessage(ctx, op)
}

func (op SaveQueueMessage) entityKey() entityKey {
	return entityKey{"queue", op.Message.ID()}
}

// RemoveQueueMessage is an Operation that removes a message from the queue.
type RemoveQueueMessage struct {
	// Message is the message to remove from the queue.
	//
	// The message's revision field must be the revision of the message as
	// currently persisted, otherwise an optimistic concurrency conflict occurs
	// and the entire batch of operations is rejected.
	Message QueueMessage
}

// AcceptVisitor calls v.VisitRemoveQueueMessage().
func (op RemoveQueueMessage) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitRemoveQueueMessage(ctx, op)
}

func (op RemoveQueueMessage) entityKey() entityKey {
	return entityKey{"queue", op.Message.ID()}
}
