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

	g.Go(func() error {
		s := grpc.NewServer()
		logging.Log(e.opts.Logger, "listening for gRPC requests on %s", e.opts.ListenAddress)
		return api.Run(ctx, e.opts.ListenAddress, s)
	})

	return g.Wait()
}
