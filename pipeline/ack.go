package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
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
		env, err := sc.Session.Envelope()
		if err != nil {
			return nack(ctx, sc, bs, err, nil)
		}

		if err := next(ctx, sc); err != nil {
			return nack(ctx, sc, bs, err, env)
		}

		return ack(ctx, sc, env)
	}
}

// ack a message and logs about it.
func ack(
	ctx context.Context,
	sc *Scope,
	env *envelope.Envelope,
) error {
	var retry mlog.Icon
	if sc.Session.FailureCount() != 0 {
		retry = mlog.RetryIcon
	}

	logging.Log(
		sc.Logger,
		mlog.String(
			[]mlog.IconWithLabel{
				mlog.MessageIDIcon.WithID(env.MessageID),
				mlog.CausationIDIcon.WithID(env.CausationID),
				mlog.CorrelationIDIcon.WithID(env.CorrelationID),
			},
			[]mlog.Icon{
				mlog.InboundIcon,
				retry,
			},
			reflect.TypeOf(env.Message).String(),
			dogma.DescribeMessage(env.Message),
		),
	)

	return sc.Session.Ack(ctx)
}

// nack a message and log about it.
func nack(
	ctx context.Context,
	sc *Scope,
	bs backoff.Strategy,
	cause error,
	env *envelope.Envelope,
) error {
	delay := bs(cause, sc.Session.FailureCount())
	retry := fmt.Sprintf("next retry in %s", delay)

	if env == nil {
		logging.Log(
			sc.Logger,
			mlog.String(
				[]mlog.IconWithLabel{
					mlog.MessageIDIcon.WithID(sc.Session.MessageID()),
					mlog.CausationIDIcon.WithID(""),
					mlog.CorrelationIDIcon.WithID(""),
				},
				[]mlog.Icon{
					mlog.InboundErrorIcon,
					mlog.ErrorIcon,
				},
				"(unknown)",
				cause.Error(),
				retry,
			),
		)
	} else {
		logging.Log(
			sc.Logger,
			mlog.String(
				[]mlog.IconWithLabel{
					mlog.MessageIDIcon.WithID(env.MessageID),
					mlog.CausationIDIcon.WithID(env.CausationID),
					mlog.CorrelationIDIcon.WithID(env.CorrelationID),
				},
				[]mlog.Icon{
					mlog.InboundErrorIcon,
					mlog.ErrorIcon,
				},
				reflect.TypeOf(env.Message).String(),
				cause.Error(),
				retry,
				dogma.DescribeMessage(env.Message),
			),
		)
	}

	return sc.Session.Nack(
		ctx,
		time.Now().Add(delay),
	)

}
