package pipeline

import (
	"context"
)

// A Sink accepts a pipeline request and populates the response.
type Sink func(context.Context, Request, *Response) error

// Stage is a segment of a pipeline.
type Stage func(context.Context, Request, *Response, Sink) error

// Pipeline is a message processing pipeline.
type Pipeline func(context.Context, Request) error

// New returns a Port that notifies observers when a request results in
// messages being enqueued or events being recorded.
func New(
	next Sink,
) Pipeline {
	return func(ctx context.Context, req Request) error {
		res := &Response{}
		return next(ctx, req, res)
	}
}
