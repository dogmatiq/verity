package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/queue"
)

// QueueHandlerStub is a test implementation of the queue.Handler interface.
type QueueHandlerStub struct {
	queue.Handler

	HandleMessageFunc func(context.Context, persistence.ManagedTransaction, *envelope.Envelope) error
}

// HandleMessage handles a message obtained from the message queue.
func (h *QueueHandlerStub) HandleMessage(
	ctx context.Context,
	tx persistence.ManagedTransaction,
	env *envelope.Envelope,
) error {
	if h.HandleMessageFunc != nil {
		return h.HandleMessageFunc(ctx, tx, env)
	}

	if h.Handler != nil {
		return h.Handler.HandleMessage(ctx, tx, env)
	}

	return nil
}
