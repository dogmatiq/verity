package infix

import (
	"context"
	"fmt"
	"net"

	"github.com/dogmatiq/configkit"
	configapi "github.com/dogmatiq/configkit/api"
	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/draftspecs/messagingspec"
	"github.com/dogmatiq/infix/eventstream/networkstream"
	"github.com/dogmatiq/infix/internal/x/grpcx"
	"google.golang.org/grpc"
)

// runServer runs the listener and gRPC server.
func (e *Engine) runServer(ctx context.Context) error {
	server := grpc.NewServer(e.opts.Network.ServerOptions...)

	if err := e.registerConfigServer(ctx, server); err != nil {
		return fmt.Errorf("unable to register config gRPC server: %w", err)
	}

	if err := e.registerEventStreamServer(ctx, server); err != nil {
		return fmt.Errorf("unable to register event stream gRPC server: %w", err)
	}

	lis, err := net.Listen("tcp", e.opts.Network.ListenAddress)
	if err != nil {
		return fmt.Errorf("unable to start gRPC listener: %w", err)
	}
	defer lis.Close()

	logging.Log(
		e.logger,
		"listening for API requests on %s",
		e.opts.Network.ListenAddress,
	)

	err = grpcx.Serve(ctx, lis, server)
	return fmt.Errorf("gRPC server stopped: %w", err)
}

// registerConfigServer registers the Config server with the gRPC server.
func (e *Engine) registerConfigServer(ctx context.Context, s *grpc.Server) error {
	// Convert slice of RichApplication to slice of Application so that we can
	// pass it to the RegisterServer() function.
	var configs []configkit.Application
	for _, cfg := range e.opts.AppConfigs {
		configs = append(configs, cfg)
	}

	configapi.RegisterServer(s, configs...)

	return nil
}

// registerConfigServer registers the EventStream server with the gRPC server.
func (e *Engine) registerEventStreamServer(ctx context.Context, s *grpc.Server) error {
	var options []networkstream.ServerOption

	for k, a := range e.apps {
		options = append(
			options,
			networkstream.WithApplication(
				k,
				a.DataStore.EventStoreRepository(),
				a.Config.
					MessageTypes().
					Produced.
					FilterByRole(message.EventRole),
			),
		)
	}

	networkstream.RegisterServer(
		s,
		e.opts.Marshaler,
		options...,
	)

	return nil
}

// runDiscoverer runs the gRPC server discovery system.
func (e *Engine) runDiscoverer(ctx context.Context) error {
	logger := discoveryLogger{e.logger}

	inspector := &discovery.Inspector{
		Observer: discovery.NewApplicationObserverSet(
			logger,
			&discovery.ApplicationExecutor{Task: e.runDiscoveredApp},
		),
		Ignore: e.ignoreDiscoveredApp,
	}

	connector := &discovery.Connector{
		Observer: discovery.NewClientObserverSet(
			logger,
			&discovery.ClientExecutor{
				Task: func(ctx context.Context, c *discovery.Client) {
					inspector.Run(ctx, c)

					// TODO: why is this needed? it's probably a remnant from
					// when logging was performed with a defer.
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
					// TODO: add convenience methods to configkit for adding tasks
					// to observer sets, including tasks that return errors. Perhaps
					// a task-with-error could panic if the error is not
					// context.Canceled.
					connector.Run(ctx, t)
				},
			},
		),
	)

	return fmt.Errorf("discoverer stopped: %w", err)
}

// ignoreDiscoveredApp returns true if the discovered application should be
// ignored because it is hosted by this engine.
func (e *Engine) ignoreDiscoveredApp(a *discovery.Application) bool {
	if _, ok := e.apps[a.Identity().Key]; !ok {
		return false
	}

	logging.Debug(
		e.logger,
		"ignoring remote %s application at %s because it is hosted by this engine",
		a.Identity(),
		a.Client.Target.Name,
	)

	return true
}

// runDiscoveredApp runs whatever processes need to run on this engine as a
// result of discovering a remote application.
func (e *Engine) runDiscoveredApp(ctx context.Context, a *discovery.Application) {
	// err will only ever be context cancelation, as
	// stream.Consumer always retries until ctx is canceled.
	_ = e.runStreamConsumersForEachApp(
		ctx,
		&networkstream.Stream{
			App:       a.Identity(),
			Client:    messagingspec.NewEventStreamClient(a.Client.Connection),
			Marshaler: e.opts.Marshaler,
			// TODO: https://github.com/dogmatiq/infix/issues/76
			// Make pre-fetch buffer size configurable.
			PreFetch: 10,
		},
	)
}

// discoveryLogger logs about discovery-related events.
type discoveryLogger struct {
	Logger logging.Logger
}

func (l discoveryLogger) TargetAvailable(t *discovery.Target) {
	logging.Debug(
		l.Logger,
		"discovered API server at %s",
		t.Name,
	)
}

func (l discoveryLogger) TargetUnavailable(t *discovery.Target) {
	logging.Debug(
		l.Logger,
		"lost API server at %s",
		t.Name,
	)
}

func (l discoveryLogger) ClientConnected(c *discovery.Client) {
	logging.Debug(
		l.Logger,
		"connected to API server at %s",
		c.Target.Name,
	)
}

func (l discoveryLogger) ClientDisconnected(c *discovery.Client) {
	logging.Debug(
		l.Logger,
		"disconnected from API server at %s",
		c.Target.Name,
	)
}

func (l discoveryLogger) ApplicationAvailable(a *discovery.Application) {
	logging.Log(
		l.Logger,
		"found @%s application at %s, identity key is %s",
		a.Identity().Name,
		a.Client.Target.Name,
		a.Identity().Key,
	)
}

func (l discoveryLogger) ApplicationUnavailable(a *discovery.Application) {
	logging.Log(
		l.Logger,
		"lost @%s application at %s, identity key was %s",
		a.Identity().Name,
		a.Client.Target.Name,
		a.Identity().Key,
	)
}
