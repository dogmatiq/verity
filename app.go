package infix

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/controller"
	"github.com/dogmatiq/infix/controller/process"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/x/loggingx"
	"github.com/dogmatiq/infix/projection"
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

	return e.streamEvents(
		ctx,
		cfg,
		ds.EventStream(),
	)
}

func (e *Engine) streamEvents(
	ctx context.Context,
	source configkit.Application,
	stream eventstream.Stream,
) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, target := range e.opts.AppConfigs {
		target := target // capture loop variable

		for _, cfg := range target.RichHandlers() {
			switch h := cfg.(type) {
			case configkit.RichProcess:
				g.Go(func() error {
					return e.streamEventsToProcess(
						ctx,
						source,
						stream,
						target,
						h,
					)
				})
			case configkit.RichProjection:
				g.Go(func() error {
					return e.streamEventsToProjection(
						ctx,
						source,
						stream,
						target,
						h,
					)
				})
			}
		}
	}

	return g.Wait()
}

func (e *Engine) streamEventsToProcess(
	ctx context.Context,
	source configkit.Application,
	stream eventstream.Stream,
	target configkit.RichApplication,
	cfg configkit.RichProcess,
) error {
	logger := e.loggerForConsumer(source, target, cfg)

	ds, err := e.dataStores.Get(ctx, target)
	if err != nil {
		return err
	}

	// TODO: this should be initialized once per application.
	packer := envelope.NewPackerForApplication(target, e.opts.Marshaler)

	c := &eventstream.Consumer{
		Stream:     stream,
		EventTypes: cfg.MessageTypes().Consumed,
		Handler: &controller.StreamAdaptor{
			Handler: cfg.Identity(),
			Controller: &process.Controller{
				HandlerConfig:  cfg,
				Packer:         packer,
				DefaultTimeout: e.opts.MessageTimeout,
				Logger:         logger,
			},
			Repository: ds.OffsetRepository(),
		},
		BackoffStrategy: e.opts.MessageBackoff,
		Logger:          logger,
	}

	return c.Run(ctx)
}

func (e *Engine) streamEventsToProjection(
	ctx context.Context,
	source configkit.Application,
	stream eventstream.Stream,
	target configkit.Application,
	cfg configkit.RichProjection,
) error {
	logger := e.loggerForConsumer(source, target, cfg)

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

func (e *Engine) loggerForConsumer(
	source configkit.Application,
	target configkit.Application,
	cfg configkit.Handler,
) logging.Logger {
	if target.Identity() == source.Identity() {
		return loggingx.WithPrefix(
			e.opts.Logger,
			"@%s | stream -> %s | ",
			target.Identity().Name,
			cfg.Identity().Name,
		)
	}

	return loggingx.WithPrefix(
		e.opts.Logger,
		"@%s | stream@%s -> %s | ",
		target.Identity().Name,
		source.Identity().Name,
		cfg.Identity().Name,
	)
}
