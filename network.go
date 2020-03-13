package infix

import (
	"context"
	"fmt"
	"net"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/api"
	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/internal/x/grpcx"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// runNetwork starts the engine's networking subsystems.
func runNetwork(ctx context.Context, opts *engineOptions) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return serve(ctx, opts)
	})

	g.Go(func() error {
		return discover(ctx, opts, &discovery.ApplicationObserverSet{}) // TODO
	})

	return g.Wait()
}

// serve runs the gRPC server.
func serve(ctx context.Context, opts *engineOptions) error {
	s := grpc.NewServer(opts.Network.ServerOptions...)

	// config []RichApplication to []Application
	configs := make([]configkit.Application, len(opts.AppConfigs))
	for i, cfg := range opts.AppConfigs {
		configs[i] = cfg
	}
	api.RegisterServer(s, configs...)

	lis, err := net.Listen("tcp", opts.Network.ListenAddress)
	if err != nil {
		return fmt.Errorf("unable to start gRPC listener: %w", err)
	}
	defer lis.Close()

	logging.Log(
		opts.Logger,
		"listening for API requests on %s",
		opts.Network.ListenAddress,
	)

	err = grpcx.Serve(ctx, lis, s)
	return fmt.Errorf("gRPC server stopped: %w", err)
}

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
				<-ctx.Done()
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
