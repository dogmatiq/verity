package persistence

import (
	"context"
	"strings"

	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

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

// AcceptVisitor calls v.VisitSaveQueueItem().
func (op SaveQueueItem) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitSaveQueueItem(ctx, op)
}

// AcceptVisitor calls v.VisitRemoveQueueItem().
func (op RemoveQueueItem) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitRemoveQueueItem(ctx, op)
}

// entityKey uniquely identifies the entity that is affected by an operation.
type entityKey [3]string

func (k entityKey) String() string {
	return strings.TrimSpace(
		strings.Join(k[:], " "),
	)
}

func (op SaveQueueItem) entityKey() entityKey {
	return entityKey{"queue", op.Item.ID()}
}

func (op RemoveQueueItem) entityKey() entityKey {
	return entityKey{"queue", op.Item.ID()}
}
