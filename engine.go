package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"golang.org/x/sync/errgroup"
)

// Engine hosts a Dogma application.
type Engine struct {
	cfg  configkit.RichApplication
	opts *engineOptions
}

// New returns a new engine that hosts the given application.
func New(app dogma.Application, options ...EngineOption) *Engine {
	cfg := configkit.FromApplication(app)

	return &Engine{
		cfg:  cfg,
		opts: resolveOptions(cfg, options),
	}
}

// Run hosts the given application until ctx is canceled or an error occurs.
func (e *Engine) Run(ctx context.Context) (err error) {
	g, ctx := errgroup.WithContext(ctx)

	id := e.cfg.Identity()
	logging.Log(e.opts.Logger, "hosting '%s' application (%s)", id.Name, id.Key)

	g.Go(func() error {
		return e.discover(ctx)
	})

	err = g.Wait()
	logging.Log(e.opts.Logger, "engine stopped: %s", err)
	return err
}

func (e *Engine) discover(ctx context.Context) error {
	if e.opts.Discoverer == nil {
		return nil
	}

	c := &discovery.Connector{
		Observer:        &discovery.ClientObserverSet{}, // TODO
		Dial:            e.opts.Dialer,
		BackoffStrategy: e.opts.BackoffStrategy, // TODO: separate for strategy for message handling
		Logger:          e.opts.Logger,
	}

	x := &discovery.TargetExecutor{
		Task: func(ctx context.Context, t *discovery.Target) {
			logging.Debug(e.opts.Logger, "config api '%s' target is available", t.Name)
			defer logging.Debug(e.opts.Logger, "config api '%s' target is unavailable", t.Name)

			// Note, Watch() only returns when ctx is canceled, so err is always
			// a context-related error.
			c.Watch(ctx, t)
		},
	}

	err := e.opts.Discoverer(ctx, x)
	return fmt.Errorf("discoverer stopped: %w", err)
}
