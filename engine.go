package verity

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/verity/internal/x/loggingx"
	"github.com/dogmatiq/verity/persistence"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Engine hosts a Dogma application.
type Engine struct {
	opts       *engineOptions
	dataStores *persistence.DataStoreSet
	semaphore  *semaphore.Weighted
	logger     logging.Logger

	apps      map[string]*app
	executors map[message.Type]dogma.CommandExecutor
	ready     chan struct{}
}

// Run creates and runs a new engine that hosts the given application.
//
// It runs until ctx is canceled or an error occurs.
func Run(ctx context.Context, app dogma.Application, options ...EngineOption) error {
	return New(app, options...).Run(ctx)
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
		semaphore: semaphore.NewWeighted(int64(opts.ConcurrencyLimit)),
		logger: loggingx.WithPrefix(
			opts.Logger,
			"engine  ",
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

	if x, ok := e.executors[mt]; ok {
		return x.ExecuteCommand(ctx, m)
	}

	return fmt.Errorf("no application accepts %s commands", mt)
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
			return e.runServer(ctx)
		})

		g.Go(func() error {
			return e.runDiscoverer(ctx)
		})
	}

	for _, a := range e.apps {
		a := a // capture loop variable

		g.Go(func() error {
			return a.Queue.Run(ctx)
		})

		g.Go(func() error {
			return e.runQueueForApp(ctx, a)
		})

		g.Go(func() error {
			return e.runStreamConsumersForEachApp(ctx, a.EventStream)
		})

		g.Go(func() error {
			return e.runCompactorsForApp(ctx, a)
		})
	}

	err := g.Wait()

	if parent.Err() != nil {
		return parent.Err()
	}

	return err
}
