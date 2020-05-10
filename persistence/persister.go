package persistence

import (
	"context"
	"fmt"

	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
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

// Batch is a set of operations that are committed to the data store atomically
// using a Persister.
type Batch []Operation

// MustValidate panics if the batch contains any operations that operate on the
// same entity.
func (batch Batch) MustValidate() {
	for i, a := range batch {
		ak := a.entityKey()

		for _, b := range batch[i+1:] {
			bk := b.entityKey()

			if ak == bk {
				panic(fmt.Sprintf(
					"batch contains multiple operations for the same entity (%s)",
					ak,
				))
			}
		}
	}
}

// Result is the result of a successfully persisted batch of operations.
type Result struct {
	// EventStoreItems contains the events from SaveEvent operations.
	EventStoreItems []*eventstore.Item
}

// ConflictError is an error indicating one or more operations within a batch
// caused an optimistic concurrency conflict.
type ConflictError struct {
	// Cause is the operation that caused the conflict.
	Cause Operation
}

func (e ConflictError) Error() string {
	return fmt.Sprintf(
		"optimistic concurrency conflict in %T operation",
		e.Cause,
	)
}
