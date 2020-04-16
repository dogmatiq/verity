package pipeline

import (
	"context"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/linger/backoff"
)

// Acknowledge returns a pipeline stage that acknowledges requests that are
// processed successfully.
//
// If next() returns an error the request is negatively-acknowledged with a
// "next-attempt" timestamp computed using the given backoff strategy.
//
// If bs is nil, backoff.DefaultStrategy is used.
func Acknowledge(
	bs backoff.Strategy,
	l logging.Logger,
) Stage {
	if bs == nil {
		bs = backoff.DefaultStrategy
	}

	return func(ctx context.Context, req Request, res *Response, next Sink) error {
		mlog.LogConsume(
			l,
			req.Envelope(),
			req.FailureCount(),
		)

		err := next(ctx, req, res)

		if err == nil {
			return req.Ack(ctx)
		}

		delay := bs(err, req.FailureCount())

		mlog.LogNack(
			l,
			req.Envelope(),
			err,
			delay,
		)

		return req.Nack(
			ctx,
			time.Now().Add(delay),
		)
	}
}
