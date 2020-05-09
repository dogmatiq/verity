package persistence

import (
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
