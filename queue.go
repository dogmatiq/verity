package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/infix/pipeline"
)

// runQueueConsumerForApp starts a consumer for the message queue.
func (e *Engine) runQueueConsumerForApp(
	ctx context.Context,
	a *app,
) error {
	err := pipeline.Pump(
		ctx,
		e.opts.Marshaler,
		a.Logger,
		a.Queue.Pop,
		a.Pipeline.Accept,
	)

	if err != nil {
		return fmt.Errorf(
			"stopped consuming from the queue for %s: %w",
			a.Config.Identity(),
			err,
		)
	}

	return nil
}
