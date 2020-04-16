package pipeline

import (
	"context"

	"github.com/dogmatiq/dodeca/logging"
)

// A Port is an entry-point into a pipeline.
type Port func(ctx context.Context, sess Session) error

// New returns a new port for a pipeline with the given stages.
func New(
	l logging.Logger,
	stages ...Stage,
) Port {
	p := Pipeline(stages)

	return func(
		ctx context.Context,
		sess Session,
	) error {
		return p.Accept(
			ctx,
			&Scope{
				Session: sess,
				Logger:  l,
			},
		)
	}
}
