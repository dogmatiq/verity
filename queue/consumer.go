package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/semaphore"
	"github.com/dogmatiq/linger/backoff"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

// Handler handles events consumed from a queue.
type Handler interface {
	// HandleMessage handles a message obtained from the message queue.
	HandleMessage(
		ctx context.Context,
		tx persistence.ManagedTransaction,
		env *envelope.Envelope,
	) error
}

// Consumer reads events from a queue in order to handle them.
type Consumer struct {
	// Queue is the message queue to consume.
	Queue *Queue

	// Handler is the target for the messages from the queue.
	Handler Handler

	// Semaphore is used to limit the number of messages being handled
	// concurrently.
	Semaphore semaphore.Semaphore

	// BackoffStrategy is the strategy used to delay individual messages after a
	// failure. If it is nil, backoff.DefaultStrategy is used.
	BackoffStrategy backoff.Strategy

	// Logger is the target for log messages from the consumer.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger

	group *errgroup.Group
}

// Run handles messages from the queue until an error occurs or ctx is canceled.
func (c *Consumer) Run(ctx context.Context) error {
	c.group, ctx = errgroup.WithContext(ctx)

	c.group.Go(func() error {
		return c.consume(ctx)
	})

	<-ctx.Done()
	return c.group.Wait()
}

// consume pops messages from the queue and starts a goroutine to handle each
// one. It waits for c.Semaphore before starting each goroutine.
func (c *Consumer) consume(ctx context.Context) error {
	logging.LogString(
		c.Logger,
		"consuming messages from queue",
	)

	for {
		sess, err := c.Queue.Pop(ctx)
		if err != nil {
			return err
		}

		if err := c.Semaphore.Acquire(ctx); err != nil {
			return multierr.Append(
				err,
				sess.Close(),
			)
		}

		c.group.Go(func() error {
			defer c.Semaphore.Release()
			return c.process(ctx, sess)
		})
	}
}

// process handles the message in sess, and commits or rolls-back as appropriate.
func (c *Consumer) process(ctx context.Context, sess *Session) error {
	defer sess.Close()

	if err := c.handle(ctx, sess); err != nil {
		s := c.BackoffStrategy
		if s == nil {
			s = backoff.DefaultStrategy
		}

		delay := s(err, 0 /* TODO: get failure count from queue */)

		logging.Log(
			c.Logger,
			"delaying next attempt of %s for %s: %s",
			sess.MessageID(),
			delay,
			err.Error(),
		)

		return sess.Rollback(
			ctx,
			time.Now().Add(delay),
		)
	}

	return sess.Commit(ctx)
}

// handle calls c.Handler with the message from sess.
func (c *Consumer) handle(ctx context.Context, sess *Session) error {
	tx, err := sess.Tx(ctx)
	if err != nil {
		return err
	}

	env, err := sess.Envelope()
	if err != nil {
		return err
	}

	return c.Handler.HandleMessage(ctx, tx, env)
}
