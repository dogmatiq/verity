package persistence

import (
	"context"
	"fmt"
)

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
