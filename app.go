package infix

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/app/projection"
	"github.com/dogmatiq/infix/internal/x/loggingx"
	"github.com/dogmatiq/infix/persistence"
	"golang.org/x/sync/errgroup"
)

func (e *Engine) runApplication(
	ctx context.Context,
	cfg configkit.RichApplication,
) error {
	logging.Log(
		e.logger,
		"starting '%s' application, identity key is %s",
		cfg.Identity().Name,
		cfg.Identity().Key,
	)

	ds, err := e.dataStores.Get(ctx, cfg)
	if err != nil {
		return err
	}

	stream, err := ds.EventStream(ctx)
	if err != nil {
		return err
	}

	return e.streamEvents(ctx, cfg, stream)
}

func (e *Engine) streamEvents(
	ctx context.Context,
	source configkit.Application,
	stream persistence.Stream,
) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, target := range e.opts.AppConfigs {
		target := target // capture loop variable

		for _, cfg := range target.RichHandlers().Projections() {
			cfg := cfg // capture loop variable

			g.Go(func() error {
				return e.runProjector(
					ctx,
					target,
					source,
					stream,
					cfg,
				)
			})
		}
	}

	return g.Wait()
}

func (e *Engine) runProjector(
	ctx context.Context,
	target configkit.Application,
	source configkit.Application,
	stream persistence.Stream,
	cfg configkit.RichProjection,
) error {
	prefix := "%s app | %s handler | %s stream | "
	if source.Identity() == target.Identity() {
		prefix = "%s app | %s handler | %s stream (self) | "
	}

	p := &projection.Projector{
		SourceApplicationKey: source.Identity().Key,
		Stream:               stream,
		ProjectionConfig:     cfg,
		BackoffStrategy:      e.opts.MessageBackoff,
		DefaultTimeout:       e.opts.MessageTimeout,
		Logger: loggingx.WithPrefix(
			e.opts.Logger,
			prefix,
			target.Identity().Name,
			cfg.Identity().Name,
			source.Identity().Name,
		),
	}

	return p.Run(ctx)
}
