package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/internal/x/loggingx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/semaphore"
	"golang.org/x/sync/errgroup"
)

// Engine hosts a Dogma application.
type Engine struct {
	opts       *engineOptions
	dataStores *persistence.DataStoreSet
	semaphore  semaphore.Semaphore // TODO: make size configurable
	logger     logging.Logger

	appsByKey     map[string]*app
	appsByCommand map[message.Type]*app
	ready         chan struct{}
}

// New returns a new engine that hosts the given application.
//
// app is the Dogma application to host on the engine. It may be nil, in which
// case at least one WithApplication() option must be specified.
func New(app dogma.Application, options ...EngineOption) *Engine {
	if app != nil {
		options = append(options, WithApplication(app))
	}

	opts := resolveEngineOptions(options...)

	return &Engine{
		opts: opts,
		dataStores: &persistence.DataStoreSet{
			Provider: opts.PersistenceProvider,
		},
		logger: loggingx.WithPrefix(
			opts.Logger,
			"engine | ",
		),
		ready: make(chan struct{}),
	}
}

// ExecuteCommand enqueues a command for execution.
func (e *Engine) ExecuteCommand(ctx context.Context, m dogma.Message) error {
	select {
	case <-e.ready:
	case <-ctx.Done():
		return ctx.Err()
	}

	mt := message.TypeOf(m)

	a, ok := e.appsByCommand[mt]
	if !ok {
		return fmt.Errorf("no application accepts %s commands", mt)
	}

	env := a.ExternalPacker.PackCommand(m)

	return a.Queue.Enqueue(ctx, env)
}

// Run hosts the given application until ctx is canceled or an error occurs.
func (e *Engine) Run(ctx context.Context) error {
	defer e.dataStores.Close()

	for _, cfg := range e.opts.AppConfigs {
		if err := e.initApp(ctx, cfg); err != nil {
			return err
		}
	}

	close(e.ready)

	return e.run(ctx)
}

func (e *Engine) run(ctx context.Context) error {
	parent := ctx
	g, ctx := errgroup.WithContext(ctx)

	if e.opts.Network != nil {
		g.Go(func() error {
			return e.serve(ctx)
		})

		g.Go(func() error {
			return e.discover(ctx)
		})
	}

	for _, a := range e.appsByKey {
		a := a // capture loop variable

		g.Go(func() error {
			// TODO: start queue consumer
			return nil
		})

		g.Go(func() error {
			return e.consumeStream(ctx, a.Stream)
		})
	}

	err := g.Wait()

	if parent.Err() != nil {
		return parent.Err()
	}

	return err
}
