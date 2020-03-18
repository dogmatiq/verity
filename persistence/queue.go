package persistence

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/envelope"
)

// A Queue is a set of messages that are yet to be handled.
type Queue interface {
	// Get returns a transaction for a message that is ready to be handled.
	Get(ctx context.Context) (QueueTransaction, error)
}

// QueueTransaction exposes persistence operations that can be performed
// atomically in order to handle a single message.
type QueueTransaction interface {
	// Envelope returns the envelope containing the message to be handled
	Envelope(ctx context.Context) (*envelope.Envelope, error)

	// Apply applies the changes from the transaction.
	Apply(ctx context.Context) error

	// Abort cancels the transaction, returning the message to the queue.
	//
	// next indicates when the message should be retried.
	Abort(ctx context.Context, next time.Time) error

	// Close closes the transaction.
	Close() error
}
