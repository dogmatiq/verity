package queuestore

import (
	"context"
	"errors"
)

// ErrConflict is returned by transaction operations when a queued message can
// not be modified because its revision is out of date.
var ErrConflict = errors.New("an optimistic concurrency conflict occured while persisting to the queue store")

// Transaction defines the primitive persistence operations for manipulating the
// message queue.
type Transaction interface {
	// SaveMessageToQueue persists a message to the application's message queue.
	//
	// If the message is already on the queue its meta-data is updated.
	//
	// i.Revision must be the revision of the message as currently persisted,
	// otherwise an optimistic concurrency conflict has occurred, the message
	// is not saved and ErrConflict is returned.
	SaveMessageToQueue(
		ctx context.Context,
		i *Item,
	) error

	// RemoveMessageFromQueue removes a specific message from the application's
	// message queue.
	//
	// i.Revision must be the revision of the message as currently persisted,
	// otherwise an optimistic concurrency conflict has occurred, the message
	// remains on the queue and ErrConflict is returned.
	RemoveMessageFromQueue(
		ctx context.Context,
		i *Item,
	) error
}
