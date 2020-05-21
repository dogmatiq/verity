package persistence

import (
	"context"
	"fmt"
	"strings"
)

// A Persister is an interface for committing batches of atomic operations to
// the data store.
type Persister interface {
	// Persist commits a batch of operations atomically.
	//
	// If any one of the operations causes an optimistic concurrency conflict
	// the entire batch is aborted and a ConflictError is returned.
	Persist(context.Context, Batch) (Result, error)
}

// Result is the result of a successfully persisted batch of operations.
type Result struct {
	// EventOffset contains the offsets of the events saved within the batch,
	// keyed by their message ID.
	EventOffsets map[string]uint64
}

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

// OperationVisitor visits persistence operations.
type OperationVisitor interface {
	VisitSaveAggregateMetaData(context.Context, SaveAggregateMetaData) error
	VisitSaveEvent(context.Context, SaveEvent) error
	VisitSaveQueueMessage(context.Context, SaveQueueMessage) error
	VisitRemoveQueueMessage(context.Context, RemoveQueueMessage) error
	VisitSaveOffset(context.Context, SaveOffset) error
}

// Batch is a set of operations that are committed to the data store atomically
// using a Persister.
type Batch []Operation

// MustValidate panics if the batch contains any operations that operate on the
// same entity.
func (b Batch) MustValidate() {
	for i, x := range b {
		xk := x.entityKey()

		for _, y := range b[i+1:] {
			yk := y.entityKey()

			if xk == yk {
				panic(fmt.Sprintf(
					"batch contains multiple operations for the same entity (%s)",
					xk,
				))
			}
		}
	}
}

// AcceptVisitor visits each operation in the batch.
func (b Batch) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	for _, op := range b {
		if err := op.AcceptVisitor(ctx, v); err != nil {
			return err
		}
	}

	return nil
}

// entityKey uniquely identifies the entity that is affected by an operation.
type entityKey [3]string

func (k entityKey) String() string {
	return strings.TrimSpace(
		strings.Join(k[:], " "),
	)
}
