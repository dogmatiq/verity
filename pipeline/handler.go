package pipeline

import (
	"context"
	"fmt"
	"reflect"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/infix/persistence"
)

// Handle returns a pipeline sink that dispatches to a handler.Handler.
func Handle(handle handler.Handler) Sink {
	return func(ctx context.Context, sc *Scope) error {
		env, err := sc.Session.Envelope()
		if err != nil {
			return err
		}

		hs := handlerScope{
			Scope: sc,
			env:   env,
		}

		return handle(ctx, hs, env)
	}
}

// handlerScope exposes a pipeline.Scope as a handler.Scope.
type handlerScope struct {
	*Scope

	env *envelope.Envelope
}

func (s handlerScope) Tx(ctx context.Context) (persistence.ManagedTransaction, error) {
	return s.Session.Tx(ctx)
}

func (s handlerScope) Log(f string, v ...interface{}) {
	logging.Log(
		s.Logger,
		mlog.String(
			[]mlog.IconWithLabel{
				mlog.MessageIDIcon.WithID(s.env.MessageID),
				mlog.CausationIDIcon.WithID(s.env.CausationID),
				mlog.CorrelationIDIcon.WithID(s.env.CorrelationID),
			},
			[]mlog.Icon{
				mlog.InboundIcon,
				mlog.IntegrationIcon, // TODO: this whole logging system needs to be moved further downstream.
			},
			reflect.TypeOf(s.env.Message).String(),
			fmt.Sprintf(f, v...),
		),
	)
}
