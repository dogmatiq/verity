package mlog

import (
	"fmt"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// LogConsume logs a message indicating that a Dogma message is being consumed.
func LogConsume(
	log logging.Logger,
	env *envelopespec.Envelope,
	fc uint,
) {
	logging.LogString(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.MetaData.MessageId),
				CausationIDIcon.WithID(env.MetaData.CausationId),
				CorrelationIDIcon.WithID(env.MetaData.CorrelationId),
			},
			[]Icon{
				ConsumeIcon,
				retryIcon(fc),
			},
			env.PortableName,
			env.MetaData.Description,
		),
	)
}

// LogProduce logs a message indicating that Dogma message is being produced.
func LogProduce(
	log logging.Logger,
	env *envelopespec.Envelope,
) {
	logging.LogString(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.MetaData.MessageId),
				CausationIDIcon.WithID(env.MetaData.CausationId),
				CorrelationIDIcon.WithID(env.MetaData.CorrelationId),
			},
			[]Icon{
				ProduceIcon,
				"",
			},
			env.PortableName,
			env.MetaData.Description,
		),
	)
}

// LogNack logs a message indicating that a session has been Nack'd.
func LogNack(
	log logging.Logger,
	env *envelopespec.Envelope,
	cause error,
	delay time.Duration,
) {
	logging.LogString(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.MetaData.MessageId),
				CausationIDIcon.WithID(env.MetaData.CausationId),
				CorrelationIDIcon.WithID(env.MetaData.CorrelationId),
			},
			[]Icon{
				ConsumeErrorIcon,
				ErrorIcon,
			},
			env.PortableName,
			cause.Error(),
			fmt.Sprintf("next retry in %s", delay),
		),
	)
}

// LogFromHandler logs an informational message produced within a Dogma handler
// via a scope.
func LogFromHandler(
	log logging.Logger,
	env *envelopespec.Envelope,
	f string, v []interface{},
) {
	logging.Log(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.MetaData.MessageId),
				CausationIDIcon.WithID(env.MetaData.CausationId),
				CorrelationIDIcon.WithID(env.MetaData.CorrelationId),
			},
			[]Icon{
				ConsumeIcon,
				"",
			},
			env.PortableName,
			fmt.Sprintf(f, v...),
		),
	)
}

func retryIcon(n uint) Icon {
	if n == 0 {
		return ""
	}

	return RetryIcon
}
