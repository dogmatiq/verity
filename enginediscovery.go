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
	linger.Limiter(0, 1*time.Hour),
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
