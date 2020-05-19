package queuestore

import (
	"errors"
)

// ErrConflict is returned by transaction operations when a queued message can
// not be modified because its revision is out of date.
var ErrConflict = errors.New("an optimistic concurrency conflict occured while persisting to the queue store")
