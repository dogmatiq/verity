package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
)

// WithApplication returns an option that hosts the given application on the
// engine.
//
// At least one WithApplication() option must be specified.
func WithApplication(app dogma.Application) EngineOption {
	return func(opts *engineOptions) {
		cfg := configkit.FromApplication(app)

		for _, c := range opts.AppConfigs {
			if c.Identity().ConflictsWith(cfg.Identity()) {
				panic(fmt.Sprintf(
					"can not host both %s and %s because they have conflicting identities",
					c.Identity(),
					cfg.Identity(),
				))
			}
		}

		opts.AppConfigs = append(opts.AppConfigs, cfg)
	}
}

func hostApplication(
	ctx context.Context,
	opts *engineOptions,
	cfg configkit.RichApplication,
) error {
	logging.Log(
		opts.Logger,
		"hosting '%s' application (%s)",
		cfg.Identity().Name,
		cfg.Identity().Key,
	)

	ds, err := opts.PersistenceProvider.Open(
		ctx,
		cfg.Identity(),
		opts.Marshaler,
	)
	if err != nil {
		return err
	}
	defer ds.Close()

	<-ctx.Done()
	return ctx.Err()
}
