package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/dodeca/logging"
)

// discover runs the API server discovery system, if configured.
func (e *Engine) discover(ctx context.Context) error {
	if e.opts.Discoverer == nil {
		return nil
	}

	i := &discovery.Inspector{
		Observer: &e.observer,
		Ignore: func(cfg configkit.Application) bool {
			for _, c := range e.configs {
				if c.Identity().ConflictsWith(cfg.Identity()) {
					return true
				}
			}

			return false
		},
	}

	c := &discovery.Connector{
		Observer: &discovery.ClientExecutor{
			Task: func(ctx context.Context, c *discovery.Client) {
				logging.Log(e.opts.Logger, "connected to API server at %s", c.Target.Name)
				defer logging.Log(e.opts.Logger, "disconnected from API server at %s", c.Target.Name)
				i.Run(ctx, c)
			},
		},
		Dial:            e.opts.Dialer,
		BackoffStrategy: e.opts.DialerBackoffStrategy,
		Logger:          e.opts.Logger,
	}

	err := e.opts.Discoverer(
		ctx,
		&discovery.TargetExecutor{
			Task: func(ctx context.Context, t *discovery.Target) {
				logging.Log(e.opts.Logger, "discovered API server at %s", t.Name)
				defer logging.Log(e.opts.Logger, "lost API server at %s", t.Name)
				c.Run(ctx, t)
			},
		},
	)

	return fmt.Errorf("discoverer stopped: %w", err)
}
