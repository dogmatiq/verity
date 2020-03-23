package persistence

import (
	"context"

	"github.com/dogmatiq/infix/envelope"
)

// A Queue is a set of command and timeout messages that are waiting to be
// handled.
type Queue interface {
	// Begin starts a transaction for handling a queued message.
	//
	// If there are no messages on the queue that are ready to be handled, no
	// transaction is started, and ok is false.
	//
	// tx is the transaction for the queued message. When it is committed the
	// message is removed from the queue.
	//
	// env is the envelope containing the queued message. env.Message may be
	// nil, and need to be populated by th caller using the data in env.Packet.
	Begin(ctx context.Context) (tx Transaction, env *envelope.Envelope, ok bool, err error)

	// Enqueue adds a message to the queue.
	Enqueue(ctx context.Context, env *envelope.Envelope) error
}
