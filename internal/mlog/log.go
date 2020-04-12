package mlog

import (
	"fmt"
	"reflect"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
)

// LogConsume logs a message indicating that a Dogma message is being consumed.
func LogConsume(
	log logging.Logger,
	env *envelope.Envelope,
	fc uint,
) {
	logging.LogString(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.MessageID),
				CausationIDIcon.WithID(env.CausationID),
				CorrelationIDIcon.WithID(env.CorrelationID),
			},
			[]Icon{
				ConsumeIcon,
				retryIcon(fc),
			},
			reflect.TypeOf(env.Message).String(),
			dogma.DescribeMessage(env.Message),
		),
	)
}

// LogProduce logs a message indicating that Dogma message is being produced.
func LogProduce(
	log logging.Logger,
	env *envelope.Envelope,
) {
	logging.LogString(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.MessageID),
				CausationIDIcon.WithID(env.CausationID),
				CorrelationIDIcon.WithID(env.CorrelationID),
			},
			[]Icon{
				ProduceIcon,
				"",
			},
			reflect.TypeOf(env.Message).String(),
			dogma.DescribeMessage(env.Message),
		),
	)
}

// LogNack logs a message indicating that a session has been Nack'd.
func LogNack(
	log logging.Logger,
	env *envelope.Envelope,
	cause error,
	delay time.Duration,
) {
	logging.LogString(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.MessageID),
				CausationIDIcon.WithID(env.CausationID),
				CorrelationIDIcon.WithID(env.CorrelationID),
			},
			[]Icon{
				ConsumeErrorIcon,
				ErrorIcon,
			},
			reflect.TypeOf(env.Message).String(),
			cause.Error(),
			fmt.Sprintf("next retry in %s", delay),
		),
	)
}

// LogNackWithoutEnvelope logs a message indicating that a session has been Nack'd when the
// message envelope is not available.
func LogNackWithoutEnvelope(
	log logging.Logger,
	id string,
	cause error,
	delay time.Duration,
) {
	logging.LogString(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(id),
				CausationIDIcon.WithID(""),
				CorrelationIDIcon.WithID(""),
			},
			[]Icon{
				ConsumeErrorIcon,
				ErrorIcon,
			},
			cause.Error(),
			fmt.Sprintf("next retry in %s", delay),
		),
	)
}

// LogFromHandler logs an informational message produced within a Dogma handler
// via a scope.
func LogFromHandler(
	log logging.Logger,
	env *envelope.Envelope,
	f string, v []interface{},
) {
	logging.Log(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.MessageID),
				CausationIDIcon.WithID(env.CausationID),
				CorrelationIDIcon.WithID(env.CorrelationID),
			},
			[]Icon{
				ConsumeIcon,
				"",
			},
			reflect.TypeOf(env.Message).String(),
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
