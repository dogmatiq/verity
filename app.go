package infix

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/internal/x/loggingx"
	"github.com/dogmatiq/infix/persistence"
)

func (e *Engine) runApplication(
	ctx context.Context,
	cfg configkit.RichApplication,
) error {
	logger := loggingx.WithPrefix(
		e.opts.Logger,
		"%s: ",
		cfg.Identity().Name,
	)

	logging.Log(
		logger,
		"application starting, identity key is %s",
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

	e.streamEvents(ctx, cfg, stream)

	return ctx.Err()
}

func (e *Engine) streamEvents(
	ctx context.Context,
	source configkit.Application,
	stream persistence.Stream,
) {
	var g sync.WaitGroup

	for _, cfg := range e.opts.AppConfigs {
		for _, hcfg := range cfg.RichHandlers() {
			hcfg := hcfg // capture loop variable

			produced, err := stream.MessageTypes(ctx)
			if err != nil {
				panic(err) // TODO
			}

			types := message.IntersectionT(
				hcfg.MessageTypes().Consumed,
				produced,
			)
			if len(types) == 0 {
				continue
			}

			g.Add(1)
			go func() {
				defer g.Done()

				e.streamEventsForHandler(
					ctx,
					stream,
					types,
					source,
					hcfg,
				)
			}()
		}
	}

	<-ctx.Done()
	g.Wait()
}

func (e *Engine) streamEventsForHandler(
	ctx context.Context,
	stream persistence.Stream,
	types message.TypeSet,
	source configkit.Application,
	hcfg configkit.RichHandler,
) {
	logging.Log(
		e.opts.Logger,
		"sourcing %d event type(s) from the '%s' application for the '%s' %s",
		len(types),
		source.Identity().Name,
		hcfg.Identity().Name,
		hcfg.HandlerType(),
	)
}
