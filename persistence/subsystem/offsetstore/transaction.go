package offsetstore

import (
	"context"
	"errors"
)

// ErrConflict is returned when the offset supplied as a parameter to method
// Transaction.SaveOffset does not equal to the currently persisted offset.
var ErrConflict = errors.New("optimistic concurrency conflict in the application event stream offset")

// Transaction defines the primitive persistence operations for manipulating
// the application event stream offset.
type Transaction interface {
	// SaveOffset persists the application event stream offset.
	//
	// If the given offset o does not equal to the currently persisted offset,
	// an optimistic concurrency conflict has occurred and ErrConflict is
	// returned.
	SaveOffset(
		ctx context.Context,
		ak string,
		o Offset,
	) error
}
