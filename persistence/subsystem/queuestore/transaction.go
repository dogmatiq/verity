package queuestore

import (
	"context"
	"errors"
)

// ErrConflict is returned by transaction operations when a queued message can
// not be modified because its revision is out of date.
var ErrConflict = errors.New("optimistic concurrency conflict on queue message")

// TODO: add tests for revision update on save

// Transaction defines the primitive persistence operations for manipulating the
// message queue.
type Transaction interface {
	// SaveMessageToQueue persists a message to the application's message queue.
	//
	// If the message is already on the queue its meta-data is updated.
	//
	// m.Revision must be the revision of the message as currently persisted,
	// otherwise an optimistic concurrency conflict has occurred, the message
	// is not saved and ErrConflict is returned.
	SaveMessageToQueue(
		ctx context.Context,
		m *Message,
	) error

	// RemoveMessageFromQueue removes a specific message from the application's
	// message queue.
	//
	// m.Revision must be the revision of the message as currently persisted,
	// otherwise an optimistic concurrency conflict has occurred, the message
	// remains on the queue and ErrConflict is returned.
	RemoveMessageFromQueue(
		ctx context.Context,
		m *Message,
	) error
}
