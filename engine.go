package infix

import (
	"context"
	"net"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"github.com/dogmatiq/infix/api"
)


// Engine hosts a Dogma application.
type Engine struct {
	app configkit.RichApplication
}

// New returns a new engine that hosts the given application.
func New(app dogma.Application, options ...EngineOption) ( *Engine) {
	cfg := configkit.FromApplication(e.Application)
	opts, err := resolveOptions(cfx, options)
	if err != nil {
		panic(err)
	}

}

// Run runs the given application.
func (e *Engine) Run(ctx context.Context) (err error) {

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return e.server
		addr := e.ListenAddress
		if addr == "" {
			addr = DefaultListenAddress
		}

		lis, err := net.Listen("tcp", addr)
		rpc := grpc.NewServer()
		rpc.Serve(lis)
	})

	return g.Wait()
}
