package infix

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/persistence"
	"golang.org/x/sync/errgroup"
)

// Engine hosts a Dogma application.
type Engine struct {
	opts       *engineOptions
	dataStores *persistence.DataStoreSet
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
			Provider:  opts.PersistenceProvider,
			Marshaler: opts.Marshaler,
		},
	}
}

// Run hosts the given application until ctx is canceled or an error occurs.
func (e *Engine) Run(ctx context.Context) error {
	defer e.dataStores.Close()

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

	for _, cfg := range e.opts.AppConfigs {
		cfg := cfg // capture loop variable

		g.Go(func() error {
			return e.runApplication(ctx, cfg)
		})
	}

	err := g.Wait()

	if parent.Err() != nil {
		return parent.Err()
	}

	return err
}
