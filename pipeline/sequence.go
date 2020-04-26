package pipeline

import (
	"context"
)

// Sequence is an ordered sequence of pipeline stages.
type Sequence []Stage

// NewSequence returns a sequence of stages as a pipeline sink.
func NewSequence(stages ...Stage) Sink {
	return Sequence(stages).Accept
}

// Accept passes the message to the next stage in the pipeline.
// It conforms to the Sink signature.
func (s Sequence) Accept(ctx context.Context, req Request, res *Response) error {
	if len(s) == 0 {
		panic("traversed the end of the sequence")
	}

	head := s[0]
	tail := s[1:]

	return head(ctx, req, res, tail.Accept)
}

// Terminate returns a stage that uses a sink to end a sequence.
func Terminate(end Sink) Stage {
	// Note this just wraps the sink in a function that matches the signature of
	// Stage, while guaranteeing to never call next().
	return func(ctx context.Context, req Request, res *Response, _ Sink) error {
		return end(ctx, req, res)
	}
}
