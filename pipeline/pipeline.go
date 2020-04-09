package pipeline

import (
	"context"
)

// A Source provides message sessions.
type Source func(context.Context) (Session, error)

// A Sink accepts message sessions.
type Sink func(context.Context, *Scope) error

// Stage is a segment of a pipeline.
type Stage func(context.Context, *Scope, Sink) error

// New returns a new pipeline.
func New(stages ...Stage) Sink {
	if len(stages) == 0 {
		panic("there must be at least one pipeline stage")
	}

	return pipeline(stages).Do
}

// pipeline is a message processing pipeline.
type pipeline []Stage

// Do processes a message using a pipeline.
// It conforms to the Sink signature.
func (p pipeline) Do(ctx context.Context, sc *Scope) error {
	if len(p) == 0 {
		panic("traversed the end of the pipeline, the last stage should always use Terminate()")
	}

	head := p[0]
	tail := p[1:]

	return head(ctx, sc, tail.Do)
}

// Terminate returns a stage that uses a sink to end a pipeline.
func Terminate(end Sink) Stage {
	// Note this just wraps the sink in a function that matches the signature of
	// Stage, while guaranteeing to never call next().
	return func(ctx context.Context, sc *Scope, _ Sink) error {
		return end(ctx, sc)
	}
}
