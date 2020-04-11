package pipeline

import (
	"context"

	"github.com/dogmatiq/infix/envelope"
)

// A Sink accepts message sessions.
type Sink func(context.Context, *Scope, *envelope.Envelope) error

// Stage is a segment of a pipeline.
type Stage func(context.Context, *Scope, *envelope.Envelope, Sink) error

// Pipeline is a message processing pipeline.
type Pipeline []Stage

// Accept processes a message using a pipeline.
// It conforms to the Sink signature.
func (p Pipeline) Accept(
	ctx context.Context,
	sc *Scope,
	env *envelope.Envelope,
) error {
	if len(p) == 0 {
		panic("traversed the end of the pipeline")
	}

	head := p[0]
	tail := p[1:]

	return head(ctx, sc, env, tail.Accept)
}

// Terminate returns a stage that uses a sink to end a pipeline.
func Terminate(end Sink) Stage {
	// Note this just wraps the sink in a function that matches the signature of
	// Stage, while guaranteeing to never call next().
	return func(
		ctx context.Context,
		sc *Scope,
		env *envelope.Envelope,
		_ Sink,
	) error {
		return end(ctx, sc, env)
	}
}
