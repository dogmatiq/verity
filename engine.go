package infix

import (
	"context"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/internal/engine"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/linger/backoff"
	"golang.org/x/sync/errgroup"
)

// Engine hosts a Dogma application.
type Engine struct {
	configs []configkit.RichApplication
	opts    *engineOptions
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

	g.Go(func() error { return e.serve(ctx) })
	g.Go(func() error { return e.discover(ctx) })

	err = g.Wait()

	logging.Log(
		e.opts.Logger,
		"engine stopped: %s",
		err,
	)

	return err
}

// server runs the gRPC server.
func (e *Engine) serve(ctx context.Context) error {
	s := &engine.Server{
		ListenAddress: e.opts.ListenAddress,
		Options:       e.opts.ServerOptions,
		Logger:        e.opts.Logger,
	}

	for _, cfg := range e.configs {
		s.Configs = append(s.Configs, cfg)
	}

	return s.Run(ctx)
}

// discover runs the API server discovery system, if configured.
func (e *Engine) discover(ctx context.Context) error {
	if e.opts.Discoverer == nil {
		return nil
	}

	d := &engine.Discoverer{
		Discover: e.opts.Discoverer,
		Observer: &discovery.ApplicationObserverSet{},
		Dialer:   e.opts.Dialer,
		BackoffStrategy: backoff.WithTransforms(
			backoff.Exponential(500*time.Millisecond),
			linger.FullJitter,
			linger.Limiter(0, 1*time.Minute),
		), // TODO: make option to configure this strategy
		ApplicationKeys: map[string]struct{}{},
		Logger:          e.opts.Logger,
	}

	for _, cfg := range e.configs {
		d.ApplicationKeys[cfg.Identity().Key] = struct{}{}
	}

	return d.Run(ctx)
}
