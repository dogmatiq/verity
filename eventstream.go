package verity

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/verity/eventstream"
	"github.com/dogmatiq/verity/handler/projection"
	"github.com/dogmatiq/verity/internal/x/loggingx"
	"github.com/dogmatiq/verity/queue"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// runStreamConsumersForEachApp runs any consumers that need to consume from s
// across all hosted applications.
func (e *Engine) runStreamConsumersForEachApp(
	ctx context.Context,
	s eventstream.Stream,
) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, a := range e.apps {
		a := a // capture loop variable
		g.Go(func() error {
			return e.runStreamConsumersForApp(ctx, s, a)
		})
	}

	return g.Wait()
}

// runStreamConsumersForApp runs any consumers that need to consume from s for
// a specific application.
func (e *Engine) runStreamConsumersForApp(
	ctx context.Context,
	s eventstream.Stream,
	a *app,
) error {
	g, ctx := errgroup.WithContext(ctx)

	if s.Application().Key != a.Config.Identity().Key {
		g.Go(func() error {
			return e.runStreamConsumerForQueue(ctx, s, a)
		})
	}

	for _, h := range a.Config.RichHandlers().Projections() {
		h := h // capture loop variable
		g.Go(func() error {
			return e.runStreamConsumerForProjection(ctx, s, a, h)
		})
	}

	return g.Wait()
}

// runStreamConsumerForQueue runs a consumer that places events from s on a's
// queue.
func (e *Engine) runStreamConsumerForQueue(
	ctx context.Context,
	s eventstream.Stream,
	a *app,
) error {
	var events *sets.Set[message.Type]

	// Find all events that are consumed by processes. Events consumed by
	// projections are not included as each projection handler has its own
	// consumer.
	for _, h := range a.Config.RichHandlers().Processes() {
		events = events.Union(
			sets.NewFromKeys(
				h.
					MessageTypes().
					Consumed(message.EventKind),
			),
		)
	}

	c := &eventstream.Consumer{
		Stream:     s,
		EventTypes: events,
		Handler: &queue.StreamAdaptor{
			Queue:            a.Queue,
			OffsetRepository: a.DataStore,
			Persister:        a.DataStore,
		},
		Semaphore:       semaphore.NewWeighted(1),
		BackoffStrategy: e.opts.MessageBackoff,
		Logger: loggingx.WithPrefix(
			e.logger,
			"[stream@%s -> queue@%s] ",
			s.Application().Name,
			a.Config.Identity().Name,
		),
	}

	if err := c.Run(ctx); err != nil {
		return fmt.Errorf(
			"stopped consuming events from %s for %s: %w",
			s.Application(),
			a.Config.Identity(),
			err,
		)
	}

	return nil
}

// runStreamConsumerForProjection runs a consumer for a specific projection.
func (e *Engine) runStreamConsumerForProjection(
	ctx context.Context,
	s eventstream.Stream,
	a *app,
	h configkit.RichProjection,
) error {
	c := &eventstream.Consumer{
		Stream: s,
		EventTypes: sets.NewFromKeys(
			h.
				MessageTypes().
				Consumed(),
		),
		Handler: &projection.StreamAdaptor{
			Identity: &envelopespec.Identity{
				Name: h.Identity().Name,
				Key:  h.Identity().Key,
			},
			Handler: h.Handler(),
			Timeout: e.opts.MessageTimeout,
			Logger:  a.Logger,
		},
		Semaphore:       e.semaphore,
		BackoffStrategy: e.opts.MessageBackoff,
		Logger: loggingx.WithPrefix(
			e.logger,
			"[stream@%s -> %s@%s] ",
			s.Application().Name,
			h.Identity().Name,
			a.Config.Identity().Name,
		),
	}

	if err := c.Run(ctx); err != nil {
		return fmt.Errorf(
			"stopped consuming events from %s for %s: %w",
			s.Application(),
			h.Identity(),
			err,
		)
	}

	return nil
}
