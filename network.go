package verity

import (
	"context"
	"fmt"
	"net"

	"github.com/dogmatiq/configkit"
	configapi "github.com/dogmatiq/configkit/api"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/discoverkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/interopspec/configspec"
	"github.com/dogmatiq/interopspec/discoverspec"
	"github.com/dogmatiq/interopspec/eventstreamspec"
	"github.com/dogmatiq/verity/eventstream/networkstream"
	"github.com/dogmatiq/verity/eventstream/persistedstream"
	"github.com/dogmatiq/verity/internal/x/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// runServer runs the listener and gRPC server.
func (e *Engine) runServer(ctx context.Context) error {
	server := grpc.NewServer(e.opts.Network.ServerOptions...)

	e.registerDiscoverAPI(server)
	e.registerConfigAPI(server)
	e.registerEventStreamAPI(server)
	reflection.Register(server)

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

// registerDiscoverServer registers the interopspec DiscoverAPI server with the
// gRPC server.
func (e *Engine) registerDiscoverAPI(s *grpc.Server) {
	ds := &discoverkit.Server{}

	for _, cfg := range e.opts.AppConfigs {
		ds.Available(cfg.Identity())
	}

	discoverspec.RegisterDiscoverAPIServer(s, ds)
}

// registerConfigAPI registers the interopspec ConfigAPI server with the gRPC
// server.
func (e *Engine) registerConfigAPI(s *grpc.Server) {
	// Convert slice of RichApplication to slice of Application so that we can
	// pass it to the NewServer() function.
	var configs []configkit.Application
	for _, cfg := range e.opts.AppConfigs {
		configs = append(configs, cfg)
	}

	configspec.RegisterConfigAPIServer(
		s,
		configapi.NewServer(configs...),
	)
}

// registerEventStreamAPI registers the EventStream server with the gRPC server.
func (e *Engine) registerEventStreamAPI(s *grpc.Server) {
	var options []networkstream.ServerOption

	for k, a := range e.apps {
		options = append(
			options,
			networkstream.WithApplication(
				k,
				&persistedstream.Stream{
					App:        a.Config.Identity(),
					Repository: a.DataStore,
					Marshaler: networkstream.NoopUnmarshaler{
						Marshaler: e.opts.Marshaler,
					},
					Cache: a.EventCache,
					// TODO: https://github.com/dogmatiq/verity/issues/76
					// Make pre-fetch buffer size configurable.
					PreFetch: 10,
					Types: a.Config.
						MessageTypes().
						Produced.
						FilterByRole(message.EventRole),
				},
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
}

// runDiscoverer runs the gRPC server discovery system.
func (e *Engine) runDiscoverer(ctx context.Context) error {
	d := &discoverkit.ApplicationDiscoverer{
		Dial:            e.opts.Network.Dialer,
		BackoffStrategy: e.opts.Network.DialerBackoff,
		LogError: func(t discoverkit.Target, err error) {
			logging.Debug(
				e.logger,
				"unable to discover applications on API server at %s: %s",
				t.Name,
				err,
			)
		},
	}

	err := e.opts.Network.Discoverer.DiscoverTargets(
		ctx,
		func(ctx context.Context, t discoverkit.Target) {
			go e.targetDiscovered(ctx, t, d)
		},
	)

	return fmt.Errorf("discoverer stopped: %w", err)
}

// targetDiscovered is called whenever a new gRPC target is discovered.
func (e *Engine) targetDiscovered(
	ctx context.Context,
	t discoverkit.Target,
	d *discoverkit.ApplicationDiscoverer,
) {
	logging.Debug(
		e.logger,
		"discovered API server at %s",
		t.Name,
	)

	defer logging.Debug(
		e.logger,
		"lost API server at %s",
		t.Name,
	)

	d.DiscoverApplications( // nolint:errcheck // error is always the context error
		ctx,
		t,
		func(ctx context.Context, a discoverkit.Application) {
			if _, ok := e.apps[a.Identity.Key]; ok {
				logging.Debug(
					e.logger,
					"ignoring remote %s application at %s because it is hosted by this engine",
					a.Identity,
					a.Target.Name,
				)

				return
			}

			go e.runDiscoveredApp(ctx, a)
		},
	)
}

// runDiscoveredApp runs whatever processes need to run on this engine as a
// result of discovering a remote application.
func (e *Engine) runDiscoveredApp(ctx context.Context, a discoverkit.Application) {
	logging.Log(
		e.logger,
		"found @%s application at %s, identity key is %s",
		a.Identity.Name,
		a.Target.Name,
		a.Identity.Key,
	)

	defer logging.Log(
		e.logger,
		"lost @%s application at %s, identity key was %s",
		a.Identity.Name,
		a.Target.Name,
		a.Identity.Key,
	)

	// err will only ever be context cancelation, as
	// stream.Consumer always retries until ctx is canceled.
	_ = e.runStreamConsumersForEachApp(
		ctx,
		&networkstream.Stream{
			App:       a.Identity,
			Client:    eventstreamspec.NewStreamAPIClient(a.Connection),
			Marshaler: e.opts.Marshaler,
			// TODO: https://github.com/dogmatiq/verity/issues/76
			// Make pre-fetch buffer size configurable.
			PreFetch: 10,
		},
	)
}
