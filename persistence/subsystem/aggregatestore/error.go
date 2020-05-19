package aggregatestore

import (
	"errors"
)

// ErrConflict is returned by transaction operations when a persisted revision
// can not be updated because the supplied "current" revision is out of date.
var ErrConflict = errors.New("an optimistic concurrency conflict occured while persisting to the aggregate store")
