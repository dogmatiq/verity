package queue

import (
	"context"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// Transaction defines the primitive persistence operations for manipulating the
// message queue.
type Transaction interface {
	// AddMessagesToQueue adds messages to the application's message queue.
	AddMessagesToQueue(
		ctx context.Context,
		envelopes []*envelopespec.Envelope,
	) error

	// DequeueMessage removes a message from the application's message queue.
	//
	// m.Revision must be the revision of the message as currently
	// persisted, otherwise an optimistic concurrency conflict has occurred, the
	// message remains on the queue and ok is false.
	DequeueMessage(
		ctx context.Context,
		m *Message,
	) (ok bool, err error)

	// UpdateQueueMessage updates meta-data about a message on the queue.
	//
	// The following fields are updated:
	//  - NextAttemptAt
	//
	// m.Revision must be the revision of the message as currently
	// persisted, otherwise an optimistic concurrency conflict has occurred, the
	// message is not updated and ok is false.
	UpdateQueueMessage(
		ctx context.Context,
		m *Message,
	) (ok bool, err error)
}
