package pipeline

import (
	"context"

	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/dogmatiq/infix/queue"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// QueueSource pops messages from a queue buffer and sends each of them via a
// pipeline in a separate goroutine.
type QueueSource struct {
	// Queue is the message queue to consume.
	Queue *queue.Queue

	// Pipeline is the messaging pipeline.
	Pipeline Pipeline

	// Semaphore is used to limit the number of messages being handled
	// concurrently.
	Semaphore *semaphore.Weighted
}

// Run sends messages from the queue via the pipeline until error occurs or ctx
// is canceled.
func (s *QueueSource) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	// Perform the actual popping logic in a goroutine managed by g. That way
	// Pop() is aborted when one of the pipeline goroutines fails.
	g.Go(func() error {
		for {
			req, err := s.Queue.Pop(ctx)
			if err != nil {
				return err
			}

			if err := s.Semaphore.Acquire(ctx, 1); err != nil {
				req.Close()
				return err
			}

			g.Go(func() error {
				defer req.Close()
				defer s.Semaphore.Release(1)
				return s.Pipeline(ctx, req)
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

// TrackWithQueue returns a pipeline observer that calls q.Track() for
// each message that is enqueued.
func TrackWithQueue(q *queue.Queue) QueueObserver {
	return func(
		ctx context.Context,
		parcels []*parcel.Parcel,
		items []*queuestore.Item,
	) error {
		for i, p := range parcels {
			if err := q.Track(ctx, p, items[i]); err != nil {
				return err
			}
		}

		return nil
	}
}
