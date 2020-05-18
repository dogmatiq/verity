package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/infix/handler"
)

// runQueueForApp processes messages from the queue.
func (e *Engine) runQueueForApp(
	ctx context.Context,
	a *app,
) error {
	c := &handler.QueueConsumer{
		Queue:           a.Queue,
		EntryPoint:      a.EntryPoint,
		Persister:       a.DataStore,
		BackoffStrategy: e.opts.MessageBackoff,
		Semaphore:       e.semaphore,
		Logger:          a.Logger,
	}

	err := c.Run(ctx)

	return fmt.Errorf(
		"stopped consuming from the queue for %s: %w",
		a.Config.Identity(),
		err,
	)
}
