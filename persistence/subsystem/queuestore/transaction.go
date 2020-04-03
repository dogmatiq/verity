package queuestore

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// Transaction defines the primitive persistence operations for manipulating the
// message queue.
type Transaction interface {
	// SaveMessageToQueue persists a message to the application's message queue.
	//
	// n indicates when the next attempt at handling the message is to be made.
	SaveMessageToQueue(
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
}
