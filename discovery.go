package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/dodeca/logging"
)

// discover runs the API server discovery system, if configured.
func discover(
	ctx context.Context,
	opts *engineOptions,
	obs discovery.ApplicationObserver,
) error {
	i := &discovery.Inspector{
		Observer: discovery.NewApplicationObserverSet(
			obs,
			&discovery.ApplicationExecutor{
				Task: func(_ context.Context, a *discovery.Application) {
					logging.Log(
						opts.Logger,
						"found %s application at %s (%s)",
						a.Identity().Name,
						a.Client.Target.Name,
						a.Identity().Key,
					)
				},
			},
		),
		Ignore: func(cfg configkit.Application) bool {
			for _, c := range opts.AppConfigs {
				if c.Identity().ConflictsWith(cfg.Identity()) {
					logging.Debug(
						opts.Logger,
						"ignoring conflicting %s application (%s) ",
						cfg.Identity().Name,
						cfg.Identity().Key,
					)

					return true
				}
			}

			return false
		},
	}

	c := &discovery.Connector{
		Observer: &discovery.ClientExecutor{
			Task: func(ctx context.Context, c *discovery.Client) {
				logging.Log(opts.Logger, "connected to API server at %s", c.Target.Name)
				defer logging.Log(opts.Logger, "disconnected from API server at %s", c.Target.Name)
				i.Run(ctx, c)
			},
		},
		Dial:            opts.Network.Dialer,
		BackoffStrategy: opts.Network.DialerBackoff,
		Logger:          opts.Logger,
	}

	err := opts.Network.Discoverer(
		ctx,
		&discovery.TargetExecutor{
			Task: func(ctx context.Context, t *discovery.Target) {
				logging.Log(opts.Logger, "discovered API server at %s", t.Name)
				defer logging.Log(opts.Logger, "lost API server at %s", t.Name)
				c.Run(ctx, t)
			},
		},
	)

	return fmt.Errorf("discoverer stopped: %w", err)
}
