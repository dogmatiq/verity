package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/infix/pipeline"
)

// runQueueForApp processes messages from the queue.
func (e *Engine) runQueueForApp(
	ctx context.Context,
	a *app,
) error {
	p := &pipeline.QueueSource{
		Queue:     a.Queue,
		Pipeline:  a.Pipeline,
		Semaphore: e.semaphore,
	}

	err := p.Run(ctx)

	return fmt.Errorf(
		"stopped consuming from the queue for %s: %w",
		a.Config.Identity(),
		err,
	)
}
