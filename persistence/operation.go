package persistence

import (
	"context"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// Operation is a persistence operation that can be performed as part of an
// atomic batch.
type Operation interface {
	// AcceptVisitor calls the appropriate visit method on the given visitor.
	AcceptVisitor(context.Context, OperationVisitor) error
}

// SaveAggregateMetaData is a persistence operation that creates or updates
// meta-data about an aggregate instance.
type SaveAggregateMetaData struct {
	// MetaData is the meta-data to persist.
	//
	// MetaData.Revision must be the revision of the instance as currently
	// persisted, otherwise an optimistic concurrency conflict occurs and the
	// entire batch of operations is rejected.
	MetaData aggregatestore.MetaData
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

// SaveOffset is a persistence operation that persists the offset of the next
// event to be consumed from a specific application.
type SaveOffset struct {
	// ApplicationKey is the identity key of the source application.
	ApplicationKey string

	// CurrentOffset must be offset currently associated with this application,
	// otherwise an optimistic concurrency conflict occurs and the entire batch
	// of operations is rejected.
	CurrentOffset uint64

	// NextOffset is the next offset to consume from this application.
	NextOffset uint64
}

// OperationVisitor visits
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
