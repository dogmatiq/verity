package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// Transaction defines the primitive persistence operations for manipulating the
// message queue.
type Transaction interface {
	// AddMessageToQueue add a message to the application's message queue.
	//
	// n indicates when the next attempt at handling the message is to be made.
	AddMessageToQueue(
		ctx context.Context,
		env *envelopespec.Envelope,
		n time.Time,
	) error

	// RemoveMessageFromQueue removes a specific message from the application's
	// message queue.
	//
	// m.Revision must be the revision of the message as currently persisted,
	// otherwise an optimistic concurrency conflict has occurred, the message
	// remains on the queue and ok is false.
	RemoveMessageFromQueue(
		ctx context.Context,
		m *Message,
	) (ok bool, err error)

	// UpdateQueueMessage updates meta-data about a message on the queue.
	//
	// The following fields are updated:
	//  - NextAttemptAt
	//
	// m.Revision must be the revision of the message as currently persisted,
	// otherwise an optimistic concurrency conflict has occurred, the message is
	// not updated and ok is false.
	UpdateQueueMessage(
		ctx context.Context,
		m *Message,
	) (ok bool, err error)
}
