package pipeline

import (
	"context"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
)

// EntryPoint starts traversal of a pipeline.
type EntryPoint func(
	context.Context,
	persistence.ManagedTransaction,
	*envelope.Envelope,
) error

// New returns a new entry-point for a pipeline with the given stages.
func New(
	m marshalkit.Marshaler,
	l logging.Logger,
	stages ...Stage,
) EntryPoint {
	p := Pipeline(stages)

	return func(
		ctx context.Context,
		tx persistence.ManagedTransaction,
		env *envelope.Envelope,
	) error {
		return p.Accept(
			ctx,
			&Scope{
				Tx:        tx,
				Marshaler: m,
				Logger:    l,
			},
			env,
		)
	}
}
