package pipeline

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/linger/backoff"
)

// Acknowledge returns a pipeline stage that acknowledges sessions that are
// processed successfully.
//
// If next() returns an error the session is negatively-acknowledged with a
// "next-attempt" timestamp computed using the given backoff strategy.
//
// If bs is nil, backoff.DefaultStrategy is used.
func Acknowledge(bs backoff.Strategy) Stage {
	if bs == nil {
		bs = backoff.DefaultStrategy
	}

	return func(ctx context.Context, sc *Scope, next Sink) error {
		mlog.LogConsume(
			sc.Logger,
			sc.Session.Envelope(),
			sc.Session.FailureCount(),
		)

		err := next(ctx, sc)

		if err == nil {
			return sc.Session.Ack(ctx)
		}

		delay := bs(err, sc.Session.FailureCount())

		mlog.LogNack(
			sc.Logger,
			sc.Session.Envelope(),
			err,
			delay,
		)

		return sc.Session.Nack(
			ctx,
			time.Now().Add(delay),
		)
	}
}
