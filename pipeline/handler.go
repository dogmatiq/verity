package pipeline

import (
	"context"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/persistence"
)

// Handle returns a pipeline sink that dispatches to a handler.Handler.
func Handle(handle handler.Handler) Sink {
	return func(ctx context.Context, sc *Scope) error {
		env, err := sc.Session.Envelope()
		if err != nil {
			return err
		}

		return handle(
			ctx,
			handlerScope{sc},
			env,
		)
	}
}

// handlerScope exposes a pipeline.Scope as a handler.Scope.
type handlerScope struct {
	*Scope
}

func (s handlerScope) Tx(ctx context.Context) (persistence.ManagedTransaction, error) {
	return s.Session.Tx(ctx)
}

func (s handlerScope) Log(f string, v ...interface{}) {
	logging.Log(s.Logger, f, v...)
}
