package infix

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/x/loggingx"
	"github.com/dogmatiq/infix/projection"
	"golang.org/x/sync/errgroup"
)

// consumeStream starts any consumers that need to consume from s across all
// hosted applications.
func (e *Engine) consumeStream(ctx context.Context, s eventstream.Stream) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, a := range e.appsByKey {
		a := a // capture loop variable
		g.Go(func() error {
			return e.consumeStreamForApp(ctx, s, a.Config)
		})
	}

	return g.Wait()
}

// consumeStreamForApp starts any consumers that need to consume from s for a
// specific application.
func (e *Engine) consumeStreamForApp(
	ctx context.Context,
	s eventstream.Stream,
	a configkit.RichApplication,
) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, h := range a.RichHandlers().Projections() {
		h := h // capture loop variable
		g.Go(func() error {
			return e.consumeStreamForProjection(ctx, s, a, h)
		})
	}

	return g.Wait()
}

// consumeStreamForApp starts any consumers that need to consume from s for a
// specific projection message handlers.
func (e *Engine) consumeStreamForProjection(
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

	return c.Run(ctx)
}
