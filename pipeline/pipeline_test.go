package pipeline_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/infix/pipeline"
)

// pass is a Sink that always returns nil.
func pass(context.Context, Request, *Response) error {
	return nil
}

// fail is a Sink that always returns an error.
func fail(context.Context, Request, *Response) error {
	return errors.New("<failed>")
}
