package infix

import (
	"context"
	"errors"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/internal/x/loggingx"
	"github.com/dogmatiq/infix/pipeline"
	"github.com/dogmatiq/infix/queue"
)

// runQueueConsumerForApp starts a consumer for the message queue.
func (e *Engine) runQueueConsumerForApp(
	ctx context.Context,
	q *queue.Queue,
	a configkit.RichApplication,
) error {
	pl := pipeline.New(
		pipeline.LimitConcurrency(e.semaphore),
		pipeline.WhenMessageEnqueued(
			func(ctx context.Context, messages []pipeline.EnqueuedMessage) error {
				for _, m := range messages {
					if err := q.Track(ctx, m.Memory, m.Persisted); err != nil {
						return err
					}
				}

				return nil
			},
		),
		pipeline.Acknowledge(e.opts.MessageBackoff),
		pipeline.Terminate(
			pipeline.Handle(
				func(ctx context.Context, sc handler.Scope, env *envelope.Envelope) error {
					// TODO: we need real handlers!
					return errors.New("the truth is, there is no handler")
				},
			),
		),
	)

	err := pipeline.Pump(
		ctx,
		e.opts.Marshaler,
		loggingx.WithPrefix(
			e.opts.Logger,
			"@%s | queue | ",
			a.Identity().Name,
		),
		q.Pop,
		pl,
	)

	if err != nil {
		return fmt.Errorf(
			"stopped consuming from the queue: %w",
			err,
		)
	}

	return nil
}
