package offsetstore

import (
	"errors"
)

// ErrConflict is returned by transaction operations when a persisted
// offset can not be updated because the supplied "current" offset is
// out of date.
var ErrConflict = errors.New("an optimistic concurrency conflict occured while persisting to the offset store")
