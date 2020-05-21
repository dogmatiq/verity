package handler

import (
	"context"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/queue"
	"github.com/dogmatiq/linger/backoff"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// QueueConsumer consumes messages from a queue and handles them.
type QueueConsumer struct {
	// Queue is the message queue to consume.
	Queue *queue.Queue

	// EntryPoint is the handler entry-point used to dispatch the message.
	EntryPoint *EntryPoint

	// Persister is the persister used to persist the units-of-work produced
	// by the entry-point, and to update the queued messages.
	Persister persistence.Persister

	// BackoffStrategy is the strategy used to delay retrying a message after a
	// failure. If it is nil, backoff.DefaultStrategy is used.
	BackoffStrategy backoff.Strategy

	// Semaphore is used to limit the number of messages being handled
	// concurrently.
	Semaphore *semaphore.Weighted

	// Logger is the target for log messages about the consumed messages.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger
}

// Run consumes messages from the queue
func (c *QueueConsumer) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	// Perform the actual popping logic in a goroutine managed by g. That way
	// Pop() is aborted when one of the pipeline goroutines fails.
	g.Go(func() error {
		for {
			m, err := c.Queue.Pop(ctx)
			if err != nil {
				return err
			}

			if err := c.Semaphore.Acquire(ctx, 1); err != nil {
				c.Queue.Requeue(m)
				return err
			}

			g.Go(func() error {
				defer c.Semaphore.Release(1)
				return c.handle(ctx, m)
			})
		}
	})

	// Don't start waiting for the group until we're actually exiting. This is
	// necessary because it's not possible to call g.Go() after g.Wait().
	<-ctx.Done()

	// Finally, wait for the group. Because our "main" popping goroutine is also
	// running under the group we will see the actual causal error, not just the
	// resulting context cancelation.
	return g.Wait()
}

func (c *QueueConsumer) handle(ctx context.Context, m queue.Message) error {
	mlog.LogConsume(
		c.Logger,
		m.Envelope,
		m.FailureCount,
	)

	return c.EntryPoint.HandleMessage(
		ctx,
		&queueAcknowledger{
			queue:     c.Queue,
			persister: c.Persister,
			backoff:   c.BackoffStrategy,
			logger:    c.Logger,
			message:   m,
		},
		m.Parcel,
	)
}

// queueAcknowledger is an implementation of Acknowledger that acknowledges
// handled messages by removing them from the queue.
type queueAcknowledger struct {
	queue     *queue.Queue
	persister persistence.Persister
	backoff   backoff.Strategy
	logger    logging.Logger
	message   queue.Message
}

func (a *queueAcknowledger) Ack(ctx context.Context, b persistence.Batch) (persistence.Result, error) {
	res, err := a.persister.Persist(
		ctx,
		append(
			b,
			persistence.RemoveQueueMessage{
				Message: a.message.QueueMessage,
			},
		),
	)
	if err != nil {
		return persistence.Result{}, err
	}

	a.queue.Remove(a.message)
	return res, nil
}

func (a *queueAcknowledger) Nack(ctx context.Context, cause error) error {
	bs := a.backoff
	if bs == nil {
		bs = backoff.DefaultStrategy
	}

	delay := bs(cause, a.message.FailureCount)
	a.message.NextAttemptAt = time.Now().Add(delay)
	a.message.FailureCount++

	mlog.LogNack(
		a.logger,
		a.message.Envelope,
		cause,
		delay,
	)

	if _, err := a.persister.Persist(
		ctx,
		persistence.Batch{
			persistence.SaveQueueMessage{
				Message: a.message.QueueMessage,
			},
		},
	); err != nil {
		return err
	}

	a.message.Revision++
	a.queue.Requeue(a.message)

	return nil
}
