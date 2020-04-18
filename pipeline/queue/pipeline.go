package queue

import (
	"context"

	"github.com/dogmatiq/infix/pipeline"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// PipelineSource pops messages from a queue and sends each of them via a
// pipeline in a separate goroutine.
type PipelineSource struct {
	// Queue is the message queue to consume.
	Queue *Queue

	// Pipeline is the entry-point of the messaging pipeline.
	Pipeline pipeline.Port

	// Semaphore is used to limit the number of messages being handled
	// concurrently.
	Semaphore *semaphore.Weighted
}

// Run sends messages from the queue via the pipeline until error occurs or ctx
// is canceled.
func (s *PipelineSource) Run(ctx context.Context) error {
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

// TrackEnqueuedCommands returns a pipeline observer that calls q.Track() for
// each message that is enqueued.
func TrackEnqueuedCommands(q *Queue) pipeline.QueueObserver {
	return func(ctx context.Context, messages []pipeline.EnqueuedMessage) error {
		for _, m := range messages {
			if err := q.Track(ctx, m.Parcel, m.Persisted); err != nil {
				return err
			}
		}

		return nil
	}
}
