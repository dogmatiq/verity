package infix

import (
	"context"
	"fmt"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/linger/backoff"
)

// Discoverer is a function that notifies an observer when a config API target
// becomes available or unavailable.
//
// It blocks until ctx is canceled or a fatal error occurs.
type Discoverer func(ctx context.Context, o discovery.TargetObserver) error

// WithDiscoverer returns an option that sets the discoverer used to find other
// engine instances.
//
// If this option is omitted or d is nil, no discovery is performed.
func WithDiscoverer(d Discoverer) EngineOption {
	return func(opts *engineOptions) {
		opts.Discoverer = d
	}
}

// DefaultDialer is the default dialer used to connect to other engine
// instances.
var DefaultDialer = discovery.DefaultDialer

// WithDialer returns an option that sets the dialer used to connect to other
// engine instances.
//
// If this option is omitted or d is nil, DefaultDialer is used.
//
// This option must be used in conjunction with WithDiscoverer().
func WithDialer(d discovery.Dialer) EngineOption {
	return func(opts *engineOptions) {
		opts.Dialer = d
	}
}

// DefaultDialerBackoffStrategy is the default backoff strategy for the dialer.
var DefaultDialerBackoffStrategy backoff.Strategy = backoff.WithTransforms(
	backoff.Exponential(100*time.Millisecond),
	linger.FullJitter,
	linger.Limiter(0, 30*time.Second),
)

// WithDialerBackoffStrategy returns an option that sets the strategy used to
// determine when the engine should retry dialing another engine instance.
//
// If this option is omitted or s is nil DefaultDialerBackoffStrategy is used.
func WithDialerBackoffStrategy(s backoff.Strategy) EngineOption {
	return func(opts *engineOptions) {
		opts.DialerBackoffStrategy = s
	}
}

// discover runs the API server discovery system, if configured.
func discover(
	ctx context.Context,
	opts *engineOptions,
	obs discovery.ApplicationObserver,
) error {
	if opts.Discoverer == nil {
		return nil
	}

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
		Dial:            opts.Dialer,
		BackoffStrategy: opts.DialerBackoffStrategy,
		Logger:          opts.Logger,
	}

	err := opts.Discoverer(
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
