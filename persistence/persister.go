package persistence

import (
	"context"
	"fmt"
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