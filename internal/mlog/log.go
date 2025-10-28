package mlog

import (
	"fmt"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
)

// LogConsume logs a message indicating that a Dogma message is being consumed.
func LogConsume(
	log logging.Logger,
	env *envelopepb.Envelope,
	fc uint,
) {
	logging.LogString(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.GetMessageId()),
				CausationIDIcon.WithID(env.GetCausationId()),
				CorrelationIDIcon.WithID(env.GetCorrelationId()),
			},
			[]Icon{
				ConsumeIcon,
				retryIcon(fc),
			},
			messageTypeName(env),
			env.GetDescription(),
		),
	)
}

// LogProduce logs a message indicating that Dogma message is being produced.
func LogProduce(
	log logging.Logger,
	env *envelopepb.Envelope,
) {
	logging.LogString(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.GetMessageId()),
				CausationIDIcon.WithID(env.GetCausationId()),
				CorrelationIDIcon.WithID(env.GetCorrelationId()),
			},
			[]Icon{
				ProduceIcon,
				"",
			},
			messageTypeName(env),
			env.GetDescription(),
		),
	)
}

// LogNack logs a message indicating that a request has been Nack'd.
func LogNack(
	log logging.Logger,
	env *envelopepb.Envelope,
	cause error,
	delay time.Duration,
) {
	logging.LogString(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.GetMessageId()),
				CausationIDIcon.WithID(env.GetCausationId()),
				CorrelationIDIcon.WithID(env.GetCorrelationId()),
			},
			[]Icon{
				ConsumeErrorIcon,
				ErrorIcon,
			},
			messageTypeName(env),
			cause.Error(),
			fmt.Sprintf("next retry in %s", delay),
		),
	)
}

// LogFromScope logs an informational message produced within a Dogma handler
// via a scope.
func LogFromScope(
	log logging.Logger,
	env *envelopepb.Envelope,
	f string, v []interface{},
) {
	logging.Log(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.GetMessageId()),
				CausationIDIcon.WithID(env.GetCausationId()),
				CorrelationIDIcon.WithID(env.GetCorrelationId()),
			},
			[]Icon{
				ConsumeIcon,
				"",
			},
			messageTypeName(env),
			fmt.Sprintf(f, v...),
		),
	)
}

// LogHandlerResult logs a debug message produced by the engine for a specific
// Dogma handler.
//
// It is designed to be used with defer.
func LogHandlerResult(
	log logging.Logger,
	env *envelopepb.Envelope,
	handler *identitypb.Identity,
	ht configkit.HandlerType,
	err *error,
	f string, v ...interface{},
) {
	if !logging.IsDebug(log) {
		return
	}

	if p := recover(); p != nil {
		// We don't want to log anything if there was a panic.
		panic(p)
	}

	messages := []string{
		handler.Name,
	}

	if *err != nil {
		messages = append(
			messages,
			(*err).Error(),
		)
	} else {
		messages = append(
			messages,
			"message handled successfully",
		)
	}

	if f != "" {
		messages = append(
			messages,
			fmt.Sprintf(f, v...),
		)
	}

	logging.Debug(
		log,
		String(
			[]IconWithLabel{
				MessageIDIcon.WithID(env.GetMessageId()),
				CausationIDIcon.WithID(env.GetCausationId()),
				CorrelationIDIcon.WithID(env.GetCorrelationId()),
			},
			[]Icon{
				HandlerTypeIcon(ht),
				errorIcon(*err),
			},
			messages...,
		),
	)
}

func errorIcon(err error) Icon {
	if err == nil {
		return ""
	}

	return ErrorIcon
}

func retryIcon(n uint) Icon {
	if n == 0 {
		return ""
	}

	return RetryIcon
}

func messageTypeName(env *envelopepb.Envelope) string {
	name := env.GetTypeId().AsString()
	if mt, ok := dogma.RegisteredMessageTypeByID(name); ok {
		name = mt.GoType().String()
	}
	return name
}
