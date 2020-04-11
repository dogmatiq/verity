package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/infix/pipeline"
	"github.com/dogmatiq/linger/backoff"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

// PipelinePump pops messages from a queue and sends them down a pipeline.
type PipelinePump struct {
	// Queue is the message queue to consume.
	Queue *Queue

	// Pipeline is the entry-point of the messaging pipeline.
	Pipeline pipeline.EntryPoint

	// Semaphore is used to limit the number of messages being handled
	// concurrently.
	Semaphore handler.Semaphore

	// BackoffStrategy is the strategy used to delay individual messages after a
	// failure. If it is nil, backoff.DefaultStrategy is used.
	BackoffStrategy backoff.Strategy

	// Logger is the target for log messages from the pump.
	Logger logging.Logger

	group *errgroup.Group
}

// Run pops messages from the queue until an error occurs or ctx is canceled.
func (p *PipelinePump) Run(ctx context.Context) error {
	p.group, ctx = errgroup.WithContext(ctx)

	p.group.Go(func() error {
		return p.run(ctx)
	})

	<-ctx.Done()
	return p.group.Wait()
}

// run pops messages from the queue and sends each of then down the pipeline in
// a new goroutine. It waits for c.Semaphore before starting each goroutine.
func (p *PipelinePump) run(ctx context.Context) error {
	for {
		m, err := p.Queue.Pop(ctx)
		if err != nil {
			return err
		}

		if err := p.Semaphore.Acquire(ctx); err != nil {
			return multierr.Append(
				err,
				m.Close(),
			)
		}

		p.group.Go(func() error {
			defer m.Close()
			defer p.Semaphore.Release()
			return p.process(ctx, m)
		})
	}
}

// process pushes the message down the pipeline, and commits or rolls-back as
// appropriate.
func (p *PipelinePump) process(ctx context.Context, m *Message) error {
	env, err := m.Envelope()
	if err != nil {
		delay, next := p.backoff(m, err)
		mlog.LogFailureWithoutEnvelope(p.Logger, m.ID(), err, delay)
		return m.Nack(ctx, next)
	}

	if err := p.send(ctx, m, env); err != nil {
		delay, next := p.backoff(m, err)
		mlog.LogFailure(p.Logger, env, err, delay)
		return m.Nack(ctx, next)
	}

	mlog.LogSuccess(p.Logger, env, m.FailureCount())
	return m.Ack(ctx)
}

// ssend pushes a message down the pipeline.
func (p *PipelinePump) send(ctx context.Context, m *Message, env *envelope.Envelope) error {
	tx, err := m.Tx(ctx)
	if err != nil {
		return err
	}

	return p.Pipeline(ctx, tx, env)
}

// backoff computes the backoff delay and next attempt time.
func (p *PipelinePump) backoff(m *Message, cause error) (time.Duration, time.Time) {
	bs := p.BackoffStrategy
	if bs == nil {
		bs = backoff.DefaultStrategy
	}

	delay := bs(cause, m.FailureCount())
	next := time.Now().Add(delay)

	return delay, next
}
