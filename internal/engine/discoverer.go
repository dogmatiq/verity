package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/linger/backoff"
)

// Discoverer encapsulates bootstrapping code for the gRPC discovery system.
type Discoverer struct {
	Discover        func(ctx context.Context, obs discovery.TargetObserver) error
	Observer        discovery.ApplicationObserver
	Dialer          discovery.Dialer
	BackoffStrategy backoff.Strategy
	ApplicationKeys map[string]struct{}
	Logger          logging.Logger
}

// Run executes the discovery system until ctx is canceled.
func (d *Discoverer) Run(ctx context.Context) error {
	i := &discovery.Inspector{
		Observer: d.Observer,
		Ignore: func(cfg configkit.Application) bool {
			_, ok := d.ApplicationKeys[cfg.Identity().Key]
			return ok
		},
	}

	c := &discovery.Connector{
		Observer: &discovery.ClientExecutor{
			Task: func(ctx context.Context, c *discovery.Client) {
				err := i.Inspect(ctx, c)

				if err != nil && !errors.Is(err, context.Canceled) {
					logging.Log(
						d.Logger,
						"could not query API server at %s: %s",
						c.Target.Name,
						err,
					)
				}
			},
		},
		Dial:            d.Dialer,
		BackoffStrategy: d.BackoffStrategy,
		Logger:          d.Logger,
	}

	err := d.Discover(
		ctx,
		&discovery.TargetExecutor{
			Task: func(ctx context.Context, t *discovery.Target) {
				logging.Log(
					d.Logger,
					"discovered API server at %s",
					t.Name,
				)

				defer logging.Log(
					d.Logger,
					"lost API server at %s",
					t.Name,
				)

				c.Watch(ctx, t)
			},
		},
	)

	return fmt.Errorf("discoverer stopped: %w", err)
}
