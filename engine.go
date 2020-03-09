package infix

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"golang.org/x/sync/errgroup"
)

// Engine hosts a Dogma application.
type Engine struct {
	configs  []configkit.RichApplication
	opts     *engineOptions
	observer discovery.ApplicationObserverSet
}

// New returns a new engine that hosts the given application.
func New(app dogma.Application, options ...EngineOption) *Engine {
	cfg := configkit.FromApplication(app)

	return &Engine{
		configs: []configkit.RichApplication{cfg},
		opts:    resolveOptions(cfg, options),
	}
}

// Run hosts the given application until ctx is canceled or an error occurs.
func (e *Engine) Run(ctx context.Context) (err error) {
	for _, cfg := range e.configs {
		logging.Log(
			e.opts.Logger,
			"hosting '%s' application (%s)",
			cfg.Identity().Name,
			cfg.Identity().Key,
		)
	}

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { return e.serveAPI(ctx) })
	g.Go(func() error { return e.discover(ctx) })

	err = g.Wait()

	logging.Log(
		e.opts.Logger,
		"engine stopped: %s",
		err,
	)

	return err
}
