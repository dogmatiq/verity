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
	group      *errgroup.Group
	dataStores map[string]persistence.DataStore
}

// New returns a new engine that hosts the given application.
//
// app is the Dogma application to host on the engine. It may be nil, in which
// case at least one WithApplication() option must be specified.
func New(app dogma.Application, options ...EngineOption) *Engine {
	if app != nil {
		options = append(options, WithApplication(app))
	}

	return &Engine{
		opts: resolveEngineOptions(options...),
	}
}

// Run hosts the given application until ctx is canceled or an error occurs.
func (e *Engine) Run(ctx context.Context) error {
	var gctx context.Context
	e.group, gctx = errgroup.WithContext(ctx)

	if err := e.setupPersistence(gctx); err != nil {
		return err
	}
	defer e.tearDownPersistence()

	e.setupNetwork(gctx)
	e.setupApplications(gctx)

	err := e.group.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	return err
}
