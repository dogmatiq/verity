package infix

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
)

func (e *Engine) hostApplication(ctx context.Context, cfg configkit.RichApplication) error {
	logging.Log(
		e.opts.Logger,
		"hosting '%s' application (%s)",
		cfg.Identity().Name,
		cfg.Identity().Key,
	)

	return nil
}
