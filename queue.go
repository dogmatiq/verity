package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/infix/queue"
)

// runQueueForApp processes messages from the queue.
func (e *Engine) runQueueForApp(
	ctx context.Context,
	a *app,
) error {
	p := &queue.PipelineSource{
		Queue:     a.Queue,
		Pipeline:  a.Pipeline.Accept,
		Semaphore: e.semaphore,
	}

	err := p.Run(ctx)

	return fmt.Errorf(
		"stopped consuming from the queue for %s: %w",
		a.Config.Identity(),
		err,
	)
}
