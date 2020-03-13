package infix

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
)

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
