package infix

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/projection"
	"golang.org/x/sync/errgroup"
)

// Engine hosts a Dogma application.
type Engine struct {
	cfg     configkit.RichApplication
	opts    *engineOptions
	streams eventstream.Registry
}

// New returns a new engine that hosts the given application.
func New(app dogma.Application, options ...EngineOption) *Engine {
	cfg := configkit.FromApplication(app)

	return &Engine{
		cfg:  cfg,
		opts: resolveOptions(cfg, options),
	}
}

// Run hosts the given application until ctx is canceled or an error occurs.
func (e *Engine) Run(ctx context.Context) (err error) {
	g, ctx := errgroup.WithContext(ctx)

	id := e.cfg.Identity()
	logging.Log(e.opts.Logger, "hosting '%s' application (%s)", id.Name, id.Key)

	// Start the gRPC server.
	g.Go(func() error {
		// s := grpc.NewServer()
		logging.Log(e.opts.Logger, "listening for gRPC requests on %s", e.opts.ListenAddress)
		// return api.Run(ctx, e.opts.ListenAddress, s)
		<-ctx.Done()
		return ctx.Err()
	})

	e.streams.RegisterStreamObserver(&projection.Supervisor{
		Context:     ctx,
		Projections: e.cfg.RichHandlers().Projections(),
		Factory: &projection.ProjectorFactory{
			Logger: e.opts.Logger,
			Meter:  e.opts.Meter,
			Tracer: e.opts.Tracer,
		},
	})

	return g.Wait()
}
