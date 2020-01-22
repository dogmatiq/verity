package infix

import (
	"context"
	"fmt"
	"net"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
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
	lis, err := net.Listen("tcp", e.opts.ListenAddress)
	if err != nil {
		return fmt.Errorf("unable to start gRPC server: %w", err)
	}
	defer lis.Close()

	rpc := grpc.NewServer()

	// GOROUTINE EXIT STRATEGY: This goroutine always blocks on ctx being
	// canceled. A "clean shutdown" is triggered by this cancelation, and a
	// "dirty shutdown" can only occur as a result of the errgroup g
	// failing, which will also cause ctx to be canceled.
	go func() {
		<-ctx.Done()
		rpc.Stop()
	}()

	logging.Log(e.opts.Logger, "listening for gRPC requests on %s", lis.Addr())

	if err := rpc.Serve(lis); err != nil {
		return fmt.Errorf("gRPC server stopped: %w", err)
	}

	// A clean exit is ultimately due to ctx being canceled.
	return ctx.Err()
}

// run starts a goroutine running fn in the given error group.
func run(ctx context.Context, g *errgroup.Group, fn func(context.Context) error) {
	g.Go(func() error {
		return fn(ctx)
	})
}
