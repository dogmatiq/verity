package persistence

import (
	"context"
	"strings"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// Operation is a persistence operation that can be performed as part of an
// atomic batch.
type Operation interface {
	// AcceptVisitor calls the appropriate visit method on the given visitor.
	AcceptVisitor(context.Context, OperationVisitor) error

	// entityKey a value that identifies the persisted "entity" that the
	// operation manipulates. No two operations in the same batch may operate
	// upon the same entity.
	entityKey() entityKey
}

// SaveEvent is a persistence operation that persists an event message.
type SaveEvent struct {
	// Envelope is the envelope containing the event to persist.
	Envelope *envelopespec.Envelope
}

// SaveQueueItem is a persistence operation that creates or updates an item on
// the message queue.
type SaveQueueItem struct {
	// Item is the item to persist to the queue.
	//
	// Item.Revision must be the revision of the item as currently persisted,
	// otherwise an optimistic concurrency conflict occurs and the entire batch
	// of operations is rejected.
	Item queuestore.Item
}

// RemoveQueueItem is a persistence operation that removes an item from the
// message queue.
type RemoveQueueItem struct {
	// Item is the item to remove from the queue.
	//
	// Item.Revision must be the revision of the item as currently persisted,
	// otherwise an optimistic concurrency conflict occurs and the entire batch
	// of operations is rejected.
	Item queuestore.Item
}

// OperationVisitor visits persistence operations.
type OperationVisitor interface {
	VisitSaveAggregateMetaData(context.Context, SaveAggregateMetaData) error
	VisitSaveEvent(context.Context, SaveEvent) error
	VisitSaveQueueItem(context.Context, SaveQueueItem) error
	VisitRemoveQueueItem(context.Context, RemoveQueueItem) error
	VisitSaveOffset(context.Context, SaveOffset) error
}

// AcceptVisitor calls v.VisitSaveAggregateMetaData().
func (op SaveAggregateMetaData) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitSaveAggregateMetaData(ctx, op)
}

// AcceptVisitor calls v.VisitSaveEvent().
func (op SaveEvent) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitSaveEvent(ctx, op)
}

// AcceptVisitor calls v.VisitSaveQueueItem().
func (op SaveQueueItem) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitSaveQueueItem(ctx, op)
}

// AcceptVisitor calls v.VisitRemoveQueueItem().
func (op RemoveQueueItem) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitRemoveQueueItem(ctx, op)
}

// AcceptVisitor calls v.VisitSaveOffset().
func (op SaveOffset) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitSaveOffset(ctx, op)
}

// entityKey uniquely identifies the entity that is affected by an operation.
type entityKey [3]string

func (k entityKey) String() string {
	return strings.TrimSpace(
		strings.Join(k[:], " "),
	)
}

func (op SaveAggregateMetaData) entityKey() entityKey {
	return entityKey{"aggregate", op.MetaData.HandlerKey, op.MetaData.InstanceID}
}

func (op SaveEvent) entityKey() entityKey {
	return entityKey{"event", op.Envelope.MetaData.MessageId}
}

func (op SaveQueueItem) entityKey() entityKey {
	return entityKey{"queue", op.Item.ID()}
}

func (op RemoveQueueItem) entityKey() entityKey {
	return entityKey{"queue", op.Item.ID()}
}

func (op SaveOffset) entityKey() entityKey {
	return entityKey{"offset", op.ApplicationKey}
}
