package processor

import (
	"context"

	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
	"golang.org/x/sync/errgroup"
)

// A Queue is an priority queue of messages.
type Queue interface {
	Pop(ctx context.Context) (Message, error)
}

type Consumer struct {
	Queue     Queue
	DataStore persistence.DataStore
	Semaphore handler.Semaphore
}

func (c *Consumer) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		for {
			m, err := c.Queue.Pop(ctx)
			if err != nil {
				return err
			}

			if err := c.Semaphore.Acquire(ctx); err != nil {
				return err
			}

			g.Go(func() error {
				defer c.Semaphore.Release()
				return c.handle(ctx, m)
			})
		}
	})

	<-ctx.Done()
	return g.Wait()
}

func (c *Consumer) handle(ctx context.Context, m *queue.Message) error {

}
