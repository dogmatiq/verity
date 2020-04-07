package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/handler/projection"
	"github.com/dogmatiq/infix/internal/x/loggingx"
	"golang.org/x/sync/errgroup"
)

// runStreamConsumersForEachApp runs any consumers that need to consume from s
// across all hosted applications.
func (e *Engine) runStreamConsumersForEachApp(
	ctx context.Context,
	s eventstream.Stream,
) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, a := range e.appsByKey {
		a := a // capture loop variable
		g.Go(func() error {
			return e.runStreamConsumersForApp(ctx, s, a.Config)
		})
	}

	return g.Wait()
}

// runStreamConsumersForApp runs any consumers that need to consume from s for
// a specific application.
func (e *Engine) runStreamConsumersForApp(
	ctx context.Context,
	s eventstream.Stream,
	a configkit.RichApplication,
) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, h := range a.RichHandlers().Projections() {
		h := h // capture loop variable
		g.Go(func() error {
			return e.runStreamConsumerForProjection(ctx, s, a, h)
		})
	}

	return g.Wait()
}

// runStreamConsumerForProjection runs a consumer for a specific projection.
func (e *Engine) runStreamConsumerForProjection(
	ctx context.Context,
	s eventstream.Stream,
	a configkit.RichApplication,
	h configkit.RichProjection,
) error {
	var logger logging.Logger

	if a.Identity() == s.Application() {
		logger = loggingx.WithPrefix(
			e.opts.Logger,
			"@%s | stream -> %s | ",
			a.Identity().Name,
			h.Identity().Name,
		)
	} else {
		logger = loggingx.WithPrefix(
			e.opts.Logger,
			"@%s | stream@%s -> %s | ",
			a.Identity().Name,
			s.Application().Name,
			h.Identity().Name,
		)
	}

	c := &eventstream.Consumer{
		Stream:     s,
		EventTypes: h.MessageTypes().Consumed,
		Handler: &projection.StreamAdaptor{
			Handler:        h.Handler(),
			DefaultTimeout: e.opts.MessageTimeout,
			Logger:         logger,
		},
		Semaphore:       e.semaphore,
		BackoffStrategy: e.opts.MessageBackoff,
		Logger:          logger,
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
