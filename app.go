package infix

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/app/projection"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/x/loggingx"
	"golang.org/x/sync/errgroup"
)

func (e *Engine) runApplication(
	ctx context.Context,
	cfg configkit.RichApplication,
) error {
	logging.Log(
		e.logger,
		"starting @%s application, identity key is %s",
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
	stream eventstream.Stream,
) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, target := range e.opts.AppConfigs {
		target := target // capture loop variable

		for _, cfg := range target.RichHandlers().Projections() {
			cfg := cfg // capture loop variable

			g.Go(func() error {
				return e.streamEventsToProjection(
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

func (e *Engine) streamEventsToProjection(
	ctx context.Context,
	target configkit.Application,
	source configkit.Application,
	stream eventstream.Stream,
	cfg configkit.RichProjection,
) error {
	var logger logging.Logger

	if target.Identity() == source.Identity() {
		logger = loggingx.WithPrefix(
			e.opts.Logger,
			"@%s | stream -> %s | ",
			target.Identity().Name,
			cfg.Identity().Name,
		)
	} else {
		logger = loggingx.WithPrefix(
			e.opts.Logger,
			"@%s | stream@%s -> %s | ",
			target.Identity().Name,
			source.Identity().Name,
			cfg.Identity().Name,
		)
	}

	c := &eventstream.Consumer{
		Stream:     stream,
		EventTypes: cfg.MessageTypes().Consumed,
		Handler: &projection.StreamAdaptor{
			Handler:        cfg.Handler(),
			DefaultTimeout: e.opts.MessageTimeout,
			Logger:         logger,
		},
		BackoffStrategy: e.opts.MessageBackoff,
		Logger:          logger,
	}

	return c.Run(ctx)
}
