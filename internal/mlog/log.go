package mlog

import (
	"fmt"
	"reflect"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
)

// LogSuccess returns the message to log when a message is handled successfully.
func LogSuccess(
	log logging.Logger,
	env *envelope.Envelope,
	failures uint,
) {
	var retry Icon
	if failures > 0 {
		retry = RetryIcon
	}

	logging.LogString(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.MessageID),
				CausationIDIcon.WithID(env.CausationID),
				CorrelationIDIcon.WithID(env.CorrelationID),
			},
			[]Icon{
				InboundIcon,
				retry,
			},
			reflect.TypeOf(env.Message).String(),
			dogma.DescribeMessage(env.Message),
		),
	)
}

// LogFailure returns the message to log when a message is not handled
// successfully.
func LogFailure(
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
				InboundErrorIcon,
				ErrorIcon,
			},
			reflect.TypeOf(env.Message).String(),
			cause.Error(),
			fmt.Sprintf("next retry in %s", delay),
			dogma.DescribeMessage(env.Message),
		),
	)
}

// LogFailureWithoutEnvelope returns the message to log when a message is not
// handled successfully because the envelope could not be unpacked.
func LogFailureWithoutEnvelope(
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
				InboundErrorIcon,
				ErrorIcon,
			},
			cause.Error(),
			fmt.Sprintf("next retry in %s", delay),
		),
	)
}
