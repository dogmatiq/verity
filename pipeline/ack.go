package pipeline

import (
	"context"
	"time"

	"github.com/dogmatiq/dodeca/logging"
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
		err := next(ctx, sc)

		if err == nil {
			return sc.Session.Ack(ctx)
		}

		delay := bs(err, sc.Session.FailureCount())

		logging.Log(
			sc.Logger,
			"delaying next attempt of %s for %s: %s",
			sc.Session.MessageID(),
			delay,
			err.Error(),
		)

		return sc.Session.Nack(
			ctx,
			time.Now().Add(delay),
		)
	}
}
