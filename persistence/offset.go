package persistence

import (
	"context"
)

// OffsetRepository is an interface for reading event stream offsets associated
// with remote applications.
type OffsetRepository interface {
	// LoadOffset loads the offset associated with a specific application.
	//
	// ak is the application's identity key.
	LoadOffset(ctx context.Context, ak string) (uint64, error)
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

// AcceptVisitor calls v.VisitSaveOffset().
func (op SaveOffset) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitSaveOffset(ctx, op)
}

func (op SaveOffset) entityKey() entityKey {
	return entityKey{"offset", op.ApplicationKey}
}
