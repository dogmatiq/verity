package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/infix/queue"
)

// runQueuePumpForApp starts a pipeline pump for the message queue.
func (e *Engine) runQueuePumpForApp(
	ctx context.Context,
	a *app,
) error {
	p := &queue.PipelinePump{
		Queue:           a.Queue,
		Pipeline:        a.Pipeline,
		Semaphore:       e.semaphore,
		BackoffStrategy: e.opts.MessageBackoff,
	}

	if err := p.Run(ctx); err != nil {
		return fmt.Errorf(
			"stopped consuming from the queue for %s: %w",
			a.Config.Identity(),
			err,
		)
	}

	return nil
}
