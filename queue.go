package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/internal/x/loggingx"
	"github.com/dogmatiq/infix/queue"
)

// runQueueConsumerForApp starts a consumer for the message queue.
func (e *Engine) runQueueConsumerForApp(
	ctx context.Context,
	q *queue.Queue,
	a configkit.RichApplication,
) error {
	c := &queue.Consumer{
		Queue:           q,
		Handler:         nil,
		Semaphore:       e.semaphore,
		BackoffStrategy: e.opts.MessageBackoff,
		Logger: loggingx.WithPrefix(
			e.opts.Logger,
			"@%s | queue | ",
			a.Identity().Name,
		),
	}

	if err := c.Run(ctx); err != nil {
		return fmt.Errorf(
			"stopped consuming from the queue: %w",
			err,
		)
	}

	return nil
}
