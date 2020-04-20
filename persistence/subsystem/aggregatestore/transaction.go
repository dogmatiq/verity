package aggregatestore

import (
	"context"
	"errors"
)

// ErrConflict is returned by transaction operations when a persisted revision
// can not be updated because the supplied "current" revision is out of date.
var ErrConflict = errors.New("an optimistic concurrency conflict occured while persisting to the aggregate store")

// Transaction defines the primitive persistence operations for manipulating the
// aggregate store.
type Transaction interface {
	// SaveAggregateMetaData persists meta-data about an aggregate instance.
	//
	// md.Revision must be the revision of the instance as currently persisted,
	// otherwise an optimistic concurrency conflict has occurred, the meta-data
	// is not saved and ErrConflict is returned.
	SaveAggregateMetaData(
		ctx context.Context,
		md *MetaData,
	) error
}
