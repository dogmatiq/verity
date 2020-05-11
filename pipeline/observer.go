package pipeline

import (
	"context"

	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/queue"
)

// Observer is a function that observes the result of a request/response
// exchange.
//
// Observers are notified whether the message is handled successfully or not. If
// err is non-nil, handling failed and the contents of r is undefined.
type Observer func(r Result, err error)

// Result is the result of a successful unit-of-work.
type Result struct {
	Events        []*eventstream.Event
	QueueMessages []queue.Message
}

// Observe returns a pipeline stage that notifies observers of the result.
func Observe(observers ...Observer) Stage {
	return func(ctx context.Context, req Request, res *Response, next Sink) error {
		err := next(ctx, req, res)

		for _, o := range observers {
			o(res.result, err)
		}

		return err
	}
}
