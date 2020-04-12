package pipeline

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/envelope"
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
		// We need the envelope in order to log about it. This also detects any
		// unmarshaling problems early in the pipeline.
		env, err := sc.Session.Envelope(ctx)
		if err != nil {
			return nack(ctx, bs, sc, nil, err)
		}

		mlog.LogConsume(
			sc.Logger,
			env,
			sc.Session.FailureCount(),
		)

		if err := next(ctx, sc); err != nil {
			return nack(ctx, bs, sc, env, err)
		}

		return sc.Session.Ack(ctx)
	}
}

// nack a message and log about it.
func nack(
	ctx context.Context,
	bs backoff.Strategy,
	sc *Scope,
	env *envelope.Envelope,
	cause error,
) error {
	delay := bs(cause, sc.Session.FailureCount())

	if env == nil {
		mlog.LogNackWithoutEnvelope(
			sc.Logger,
			sc.Session.MessageID(),
			cause,
			delay,
		)
	} else {
		mlog.LogNack(
			sc.Logger,
			env,
			cause,
			delay,
		)
	}

	return sc.Session.Nack(
		ctx,
		time.Now().Add(delay),
	)
}
