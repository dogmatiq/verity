package aggregatestore

import (
	"context"
	"errors"
)

// ErrConflict is returned by transaction operations when a persisted
// revision can not be updated because the supplied "current" revision is
// out of date.
var ErrConflict = errors.New("an optimistic concurrency conflict occured while persisting to the aggregate store")

// Transaction defines the primitive persistence operations for manipulating the
// aggregate store.
type Transaction interface {
	// IncrementAggregateRevision increments the persisted revision of a an
	// aggregate instance.
	//
	// ak is the aggregate handler's identity key, id is the instance ID.
	//
	// c must be the instance's current revision as persisted, otherwise an
	// optimistic concurrency conflict has occurred, the revision is not saved
	// and ErrConflict is returned.
	IncrementAggregateRevision(
		ctx context.Context,
		hk string,
		id string,
		c Revision,
	) error
}
