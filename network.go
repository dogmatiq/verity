package infix

import (
	"context"
	"fmt"
	"net"

	"github.com/dogmatiq/configkit"
	configapi "github.com/dogmatiq/configkit/api"
	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/api/messaging/eventstream"
	"github.com/dogmatiq/infix/internal/x/grpcx"
	"github.com/dogmatiq/infix/persistence"
	"google.golang.org/grpc"
)

// serve starts the listener and gRPC server.
func (e *Engine) serve(ctx context.Context) error {
	server := grpc.NewServer(e.opts.Network.ServerOptions...)

	if err := e.registerConfigServer(ctx, server); err != nil {
		return fmt.Errorf("unable to register config gRPC server: %w", err)
	}

	if err := e.registerEventStreamServer(ctx, server); err != nil {
		return fmt.Errorf("unable to register event-stream gRPC server: %w", err)
	}

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

// registerConfigServer registers the Config server with the gRPC server.
func (e *Engine) registerConfigServer(ctx context.Context, s *grpc.Server) error {
	var configs []configkit.Application
	for _, cfg := range e.opts.AppConfigs {
		// convert []RichApplication to []Application
		configs = append(configs, cfg)
	}

	configapi.RegisterServer(s, configs...)

	return nil
}

// registerConfigServer registers the EventStream server with the gRPC server.
func (e *Engine) registerEventStreamServer(ctx context.Context, s *grpc.Server) error {
	streams := map[string]persistence.Stream{}

	for appKey, ds := range e.dataStores {
		stream, err := ds.EventStream(ctx)
		if err != nil {
			return err
		}

		streams[appKey] = stream
	}

	eventstream.RegisterServer(
		s,
		e.opts.Marshaler,
		streams,
	)

	return nil
}

// discover starts the gRPC server discovery system.
func (e *Engine) discover(ctx context.Context) error {
	logger := discoveryLogger{e.opts.Logger}

	i := &discovery.Inspector{
		Observer: discovery.NewApplicationObserverSet(
			logger,
			&discovery.ApplicationExecutor{
				Task: func(ctx context.Context, a *discovery.Application) {
					stream := eventstream.NewEventStream(
						a.Identity().Key,
						a.Client.Connection,
						e.opts.Marshaler,
						0, // TODO: make configurable
					)

					e.streamEvents(ctx, a, stream)
				},
			},
		),
		Ignore: func(a *discovery.Application) bool {
			for _, c := range e.opts.AppConfigs {
				if c.Identity().ConflictsWith(a.Identity()) {
					logging.Debug(
						e.opts.Logger,
						"ignoring conflicting '%s' application at %s (%s) ",
						a.Identity().Name,
						a.Client.Target.Name,
						a.Identity().Key,
					)

					return true
				}
			}

			return false
		},
	}

	c := &discovery.Connector{
		Observer: discovery.NewClientObserverSet(
			logger,
			&discovery.ClientExecutor{
				Task: func(ctx context.Context, c *discovery.Client) {
					i.Run(ctx, c)
					<-ctx.Done()
				},
			},
		),
		Dial:            e.opts.Network.Dialer,
		BackoffStrategy: e.opts.Network.DialerBackoff,
	}

	err := e.opts.Network.Discoverer(
		ctx,
		discovery.NewTargetObserverSet(
			logger,
			&discovery.TargetExecutor{
				Task: func(ctx context.Context, t *discovery.Target) {
					c.Run(ctx, t)
				},
			},
		),
	)

	return fmt.Errorf("discoverer stopped: %w", err)
}

type discoveryLogger struct {
	Logger logging.Logger
}

func (l discoveryLogger) TargetAvailable(t *discovery.Target) {
	logging.Log(
		l.Logger,
		"discovered API server at %s",
		t.Name,
	)
}

func (l discoveryLogger) TargetUnavailable(t *discovery.Target) {
	logging.Log(
		l.Logger,
		"lost API server at %s",
		t.Name,
	)
}

func (l discoveryLogger) ClientConnected(c *discovery.Client) {
	logging.Log(
		l.Logger,
		"connected to API server at %s",
		c.Target.Name,
	)
}

func (l discoveryLogger) ClientDisconnected(c *discovery.Client) {
	logging.Log(
		l.Logger,
		"disconnected from API server at %s",
		c.Target.Name,
	)
}

func (l discoveryLogger) ApplicationAvailable(a *discovery.Application) {
	logging.Log(
		l.Logger,
		"found '%s' application at %s (%s)",
		a.Identity().Name,
		a.Client.Target.Name,
		a.Identity().Key,
	)
}

func (l discoveryLogger) ApplicationUnavailable(a *discovery.Application) {
}
