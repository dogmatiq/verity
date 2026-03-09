package verity

import (
	"context"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/verity/handler/projection"
	"github.com/dogmatiq/verity/internal/x/loggingx"
	"golang.org/x/sync/errgroup"
)

// runCompactorsForApp runs a compactor for each projection within a specific
// application.
func (e *Engine) runCompactorsForApp(ctx context.Context, a *app) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, h := range a.Config.Handlers() {
		if h, ok := h.(*config.Projection); ok {
			g.Go(func() error {
				return e.runCompactorForProjection(ctx, h, a)
			})
		}
	}

	return g.Wait()
}

// runCompactorForProjection runs a compactor for a specific projection.
func (e *Engine) runCompactorForProjection(
	ctx context.Context,
	h *config.Projection,
	a *app,
) error {
	c := &projection.Compactor{
		Handler:   h.Source.Get(),
		Interval:  e.opts.ProjectionCompactInterval,
		Timeout:   e.opts.ProjectionCompactTimeout,
		Semaphore: e.semaphore,
		Logger: loggingx.WithPrefix(
			e.logger,
			"[compactor %s@%s] ",
			h.Identity().Name,
			a.Config.Identity().Name,
		),
	}

	return c.Run(ctx)
}
