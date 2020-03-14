package infix

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
)

func (e *Engine) setupPersistence(ctx context.Context) error {
	e.dataStores = map[string]persistence.DataStore{}

	for _, cfg := range e.opts.AppConfigs {
		ds, err := e.opts.PersistenceProvider.Open(
			ctx,
			cfg.Identity(),
			e.opts.Marshaler,
		)
		if err != nil {
			return err
		}

		e.dataStores[cfg.Identity().Key] = ds
	}

	return nil
}

func (e *Engine) tearDownPersistence() {
	e.dataStores = map[string]persistence.DataStore{}

	for _, ds := range e.dataStores {
		ds.Close()
	}
}
