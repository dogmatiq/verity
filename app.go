package infix

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
)

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
