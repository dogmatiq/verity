package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// Transaction defines the primitive persistence operations for manipulating the
// message queue.
type Transaction interface {
	// EnqueueMessages adds messages to the application's message queue.
	EnqueueMessages(
		ctx context.Context,
		envelopes []*envelopespec.Envelope,
	) error

	// DequeueMessage removes a message from the application's message queue.
	//
	// m.Revision must be the revision of the queued message as currently
	// persisted, otherwise an optimistic concurrency conflict has occurred, the
	// message remains on the queue and ok is false.
	DequeueMessage(
		ctx context.Context,
		m *Message,
	) (ok bool, err error)

	// DelayQueuedMessage returns defers the next attempt of a queued message
	// after a failure.
	//
	// n is the time at which the next attempt at handling the message occurs.
	//
	// m.Revision must be the revision of the queued message as currently
	// persisted, otherwise an optimistic concurrency conflict has occurred, the
	// message is not delayed and ok is false.
	DelayQueuedMessage(
		ctx context.Context,
		m *Message,
		n time.Time,
	) (ok bool, err error)
}
