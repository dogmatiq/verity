package infix

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
)

// WithApplication returns an option that hosts the given application on the
// engine.
//
// At least one WithApplication() option must be specified.
func WithApplication(app dogma.Application) EngineOption {
	return func(opts *engineOptions) {
		opts.AppConfigs = append(
			opts.AppConfigs,
			configkit.FromApplication(app),
		)
	}
}
func (e *Engine) hostApplication(ctx context.Context, cfg configkit.RichApplication) error {
	logging.Log(
		e.opts.Logger,
		"hosting '%s' application (%s)",
		cfg.Identity().Name,
		cfg.Identity().Key,
	)

	return nil
}
