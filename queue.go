package infix

import (
	"context"
)

// runQueueForApp processes messages from the queue.
func (e *Engine) runQueueForApp(
	ctx context.Context,
	a *app,
) error {
	return nil // TODO
	// p := &pipeline.QueueSource{
	// 	Queue:     a.Queue,
	// 	Pipeline:  a.Pipeline,
	// 	Semaphore: e.semaphore,
	// }

	// err := p.Run(ctx)

	// return fmt.Errorf(
	// 	"stopped consuming from the queue for %s: %w",
	// 	a.Config.Identity(),
	// 	err,
	// )
}
