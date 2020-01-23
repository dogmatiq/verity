package infix

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/api"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Engine hosts a Dogma application.
type Engine struct {
	app  configkit.RichApplication
	opts *engineOptions
}

// New returns a new engine that hosts the given application.
func New(app dogma.Application, options ...EngineOption) *Engine {
	cfg := configkit.FromApplication(app)

	return &Engine{
		app:  cfg,
		opts: resolveOptions(cfg, options),
	}
}

// Run hosts the given application until ctx is canceled or an error occurs.
func (e *Engine) Run(ctx context.Context) (err error) {
	g, ctx := errgroup.WithContext(ctx)

	id := e.app.Identity()
	logging.Log(e.opts.Logger, "hosting '%s' application (%s)", id.Name, id.Key)

	run(ctx, g, e.startGRPC)

	return g.Wait()
}

// startGRPC runs the engine's gRPC server until ctx is canceled.
func (e *Engine) startGRPC(ctx context.Context) error {
	s := grpc.NewServer()

	logging.Log(e.opts.Logger, "listening for gRPC requests on %s", e.opts.ListenAddress)

	return api.Run(ctx, e.opts.ListenAddress, s)
}

// run starts a goroutine running fn in the given error group.
func run(ctx context.Context, g *errgroup.Group, fn func(context.Context) error) {
	g.Go(func() error {
		return fn(ctx)
	})
}
