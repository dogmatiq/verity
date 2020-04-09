package pipeline

import (
	"context"

	"github.com/dogmatiq/infix/handler"
)

// LimitConcurrency returns a pipeline stage that does not advance to the next
// stage until a sempahore can be acquired.
func LimitConcurrency(sem handler.Semaphore) Stage {
	return func(ctx context.Context, sc *Scope, next Sink) error {
		if err := sem.Acquire(ctx); err != nil {
			return err
		}
		defer sem.Release()

		return next(ctx, sc)
	}
}
