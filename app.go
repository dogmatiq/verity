package infix

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
)

func (e *Engine) setupApplications(ctx context.Context) {
	for _, cfg := range e.opts.AppConfigs {
		cfg := cfg // capture loop variable
		e.group.Go(func() error {
			return e.runApplication(ctx, cfg)
		})
	}
}

func (e *Engine) runApplication(
	ctx context.Context,
	cfg configkit.RichApplication,
) error {
	logging.Log(
		e.opts.Logger,
		"hosting '%s' application (%s)",
		cfg.Identity().Name,
		cfg.Identity().Key,
	)

	<-ctx.Done()
	return ctx.Err()
}
