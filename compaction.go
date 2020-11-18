package verity

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/verity/handler/projection"
	"github.com/dogmatiq/verity/internal/x/loggingx"
	"golang.org/x/sync/errgroup"
)

// runCompactorsForApp runs a compactor for each projection within a specific
// application.
func (e *Engine) runCompactorsForApp(ctx context.Context, a *app) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, h := range a.Config.RichHandlers().Projections() {
		h := h // capture loop variable
		g.Go(func() error {
			return e.runCompactorForProjection(ctx, h, a)
		})
	}

	return g.Wait()
}

// runCompactorForProjection runs a compactor for a specific projection.
func (e *Engine) runCompactorForProjection(
	ctx context.Context,
	h configkit.RichProjection,
	a *app,
) error {
	c := &projection.Compactor{
		Handler:   h.Handler(),
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
