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
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/remotestream"
	"google.golang.org/grpc"
)

func (e *Engine) setupNetwork(ctx context.Context) {
	if e.opts.Network == nil {
		return
	}

	e.group.Go(func() error {
		return e.serveAPI(ctx)
	})

	e.group.Go(func() error {
		return e.discoverAPIs(ctx)
	})
}

// serveAPI starts the listener and gRPC server.
func (e *Engine) serveAPI(ctx context.Context) error {
	var configs []configkit.Application // convert []RichApplication to []Application
	streams := map[string]persistence.Stream{}

	for _, cfg := range e.opts.AppConfigs {
		configs = append(configs, cfg)

		k := cfg.Identity().Key
		ds := e.dataStores[k]
		stream, err := ds.EventStream(ctx)
		if err != nil {
			return err
		}

		streams[k] = stream
	}

	server := grpc.NewServer(e.opts.Network.ServerOptions...)
	api.RegisterServer(server, configs...)
	remotestream.RegisterServer(server, e.opts.Marshaler, streams)

	lis, err := net.Listen("tcp", e.opts.Network.ListenAddress)
	if err != nil {
		return fmt.Errorf("unable to start gRPC listener: %w", err)
	}
	defer lis.Close()

	logging.Log(
		e.opts.Logger,
		"listening for API requests on %s",
		e.opts.Network.ListenAddress,
	)

	err = grpcx.Serve(ctx, lis, server)
	return fmt.Errorf("gRPC server stopped: %w", err)
}

// discoverAPIs starts the gRPC server discovery system.
func (e *Engine) discoverAPIs(ctx context.Context) error {
	i := &discovery.Inspector{
		Observer: discovery.NewApplicationObserverSet(
			&discovery.ApplicationExecutor{
				Task: func(_ context.Context, a *discovery.Application) {
					logging.Log(
						e.opts.Logger,
						"found %s application at %s (%s)",
						a.Identity().Name,
						a.Client.Target.Name,
						a.Identity().Key,
					)
				},
			},
		),
		Ignore: func(cfg configkit.Application) bool {
			for _, c := range e.opts.AppConfigs {
				if c.Identity().ConflictsWith(cfg.Identity()) {
					logging.Debug(
						e.opts.Logger,
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
				logging.Log(e.opts.Logger, "connected to API server at %s", c.Target.Name)
				defer logging.Log(e.opts.Logger, "disconnected from API server at %s", c.Target.Name)
				i.Run(ctx, c)
				<-ctx.Done()
			},
		},
		Dial:            e.opts.Network.Dialer,
		BackoffStrategy: e.opts.Network.DialerBackoff,
		Logger:          e.opts.Logger,
	}

	err := e.opts.Network.Discoverer(
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
