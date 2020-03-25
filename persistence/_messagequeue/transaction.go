package messagequeue

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/envelope"
)

// Transaction defines the primitive persistence operations for manipulating the
// message queue.
type Transaction interface {
	// EnqueueMessages adds messages to the application's message queue.
	EnqueueMessages(
		ctx context.Context,
		envelopes ...*envelope.Envelope,
	) error

	// DequeueMessage returns a queued message that is ready to be handled.
	//
	// n is the number of times this message has been requeued.
	//
	// If the queue is empty, or none of the queued messages are ready to be
	// handled, ok is false.
	//
	// env.Message may be nil, in which case it is the caller's responsibility
	// to unmarshal the data in env.Packet.
	DequeueMessage(
		ctx context.Context,
	) (
		env *envelope.Envelope,
		n uint,
		ok bool,
		err error,
	)

	// RequeueMessage returns a dequeued message to the queue after a failure.
	//
	// n is the time at which the next attempt at handling the message is made.
	RequeueMessage(
		ctx context.Context,
		env *envelope.Envelope,
		n time.Time,
	) error
}
