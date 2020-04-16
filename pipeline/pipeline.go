package pipeline

import (
	"context"
)

// A Sink accepts a pipeline request and populates the response.
type Sink func(context.Context, Request, *Response) error

// Stage is a segment of a pipeline.
type Stage func(context.Context, Request, *Response, Sink) error

// Port is an entry-point to a pipeline.
type Port func(context.Context, Request) error

// Pipeline is a message processing pipeline.
type Pipeline []Stage

// Accept accepts a request and returns its response.
// It conforms to the Port signature.
func (p Pipeline) Accept(ctx context.Context, req Request) error {
	res := &Response{}
	return p.do(ctx, req, res)
}

// do passes the message to the next stage in the pipeline.
func (p Pipeline) do(ctx context.Context, req Request, res *Response) error {
	if len(p) == 0 {
		panic("traversed the end of the pipeline")
	}

	head := p[0]
	tail := p[1:]

	return head(ctx, req, res, tail.do)
}

// Terminate returns a stage that uses a sink to end a pipeline.
func Terminate(end Sink) Stage {
	// Note this just wraps the sink in a function that matches the signature of
	// Stage, while guaranteeing to never call next().
	return func(ctx context.Context, req Request, res *Response, _ Sink) error {
		return end(ctx, req, res)
	}
}
