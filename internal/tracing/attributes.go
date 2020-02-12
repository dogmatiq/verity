package tracing

import (
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
	"go.opentelemetry.io/otel/api/core"
	"go.opentelemetry.io/otel/api/key"
	"go.opentelemetry.io/otel/api/trace"
)

// AttributeSource is a function that sets attributes on a span.
type AttributeSource func(trace.Span)

var (
	// HandlerNameKey is a span attribute key for they name component of a
	// handler's identity.
	HandlerNameKey = key.New("dogma.handler.name")

	// HandlerKeyKey is a span attribute key for they key component of a
	// handler's identity.
	HandlerKeyKey = key.New("dogma.handler.key")

	// HandlerTypeKey is a span attribute key for a handler type.
	HandlerTypeKey = key.New("dogma.handler.type")
)

var (
	// HandlerTypeAggregateAttr is a span attribute with the HandlerType key set
	// to "aggregate".
	HandlerTypeAggregateAttr = HandlerTypeKey.String(configkit.AggregateHandlerType.String())

	// HandlerTypeProcessAttr is a span attribute with the HandlerType key set
	// to "process".
	HandlerTypeProcessAttr = HandlerTypeKey.String(configkit.ProcessHandlerType.String())

	// HandlerTypeIntegrationAttr is a span attribute with the HandlerType key
	// set to "Integration".
	HandlerTypeIntegrationAttr = HandlerTypeKey.String(configkit.IntegrationHandlerType.String())

	// HandlerTypeProjectionAttr is a span attribute with the HandlerType key
	// set to "projection".
	HandlerTypeProjectionAttr = HandlerTypeKey.String(configkit.ProjectionHandlerType.String())
)

// HandlerAttributes returns an attribute source that sets the standard
// attributes describing a message handler.
func HandlerAttributes(cfg configkit.Handler) AttributeSource {
	attrs := []core.KeyValue{
		HandlerNameKey.String(cfg.Identity().Name),
		HandlerKeyKey.String(cfg.Identity().Key),
		HandlerTypeKey.String(cfg.HandlerType().String()),
	}

	return func(s trace.Span) {
		s.SetAttributes(attrs...)
	}
}

var (
	// MessageIDKey is a span attribute key for the ID of a message.
	MessageIDKey = key.New("dogma.message.id")

	// MessageCausationIDKey is a span attribute key for the causation ID of a message.
	MessageCausationIDKey = key.New("dogma.message.causation_id")

	// MessageCorrelationIDKey is a span attribute key for the correlation ID of a message.
	MessageCorrelationIDKey = key.New("dogma.message.correlation_id")

	// MessageTypeKey is a span attribute key for the type of a message.
	MessageTypeKey = key.New("dogma.message.type")

	// MessageRoleKey is a span attribute key for the role of a message.
	MessageRoleKey = key.New("dogma.message.role")

	// MessageDescriptionKey is a span attribute key for the human-readable
	// description of a message.
	MessageDescriptionKey = key.New("dogma.message.description")

	// MessageCreatedAtKey is a span attribute key for the "created at" time of
	// a message.
	MessageCreatedAtKey = key.New("dogma.message.created_at")

	// MessageScheduledForKey is a span attribute key for the "scheduled for"
	// time of a timeout message.
	MessageScheduledForKey = key.New("dogma.message.scheduled_for")
)

var (
	// MessageSourceApplicationName is a span attribute key for the name of a
	// message's source application.
	MessageSourceApplicationName = key.New("dogma.message.source.application.name")

	// MessageSourceApplicationKey is a span attribute key for the key of a
	// message's source application.
	MessageSourceApplicationKey = key.New("dogma.message.source.application.key")

	// MessageSourceHandlerName is a span attribute key for the name of a
	// message's source handler.
	MessageSourceHandlerName = key.New("dogma.message.source.handler.name")

	// MessageSourceHandlerKey is a span attribute key for the key of a
	// message's source handler.
	MessageSourceHandlerKey = key.New("dogma.message.source.handler.key")

	// MessageSourceInstanceID is a span attribute key for a message's source
	// instance (for aggregates and processes).
	MessageSourceInstanceID = key.New("dogma.message.source.instance_id")
)

var (
	// MessageRoleCommandAttr is a span attribute with the MessageRole key set
	// to "command".
	MessageRoleCommandAttr = MessageRoleKey.String(message.CommandRole.String())

	// MessageRoleEventAttr is a span attribute with the MessageRole key set to
	// "event".
	MessageRoleEventAttr = MessageRoleKey.String(message.EventRole.String())

	// MessageRoleTimeoutAttr is a span attribute with the MessageRole key set
	// to "timeout".
	MessageRoleTimeoutAttr = MessageRoleKey.String(message.TimeoutRole.String())
)

// MessageAttributes returns an attribute source that sets the standard
// attributes describing a message.
func MessageAttributes(env *envelope.Envelope, r message.Role) AttributeSource {
	attrs := []core.KeyValue{
		MessageIDKey.String(env.MessageID),
		MessageCausationIDKey.String(env.CausationID),
		MessageCorrelationIDKey.String(env.CorrelationID),
		MessageTypeKey.String(message.TypeOf(env.Message).String()),
		MessageRoleKey.String(r.String()),
		MessageCreatedAtKey.String(env.CreatedAt.Format(time.RFC3339)),
		MessageDescriptionKey.String(dogma.DescribeMessage(env.Message)),
		MessageSourceApplicationName.String(env.Source.Application.Name),
		MessageSourceApplicationKey.String(env.Source.Application.Key),
	}

	if r == message.TimeoutRole {
		attrs = append(
			attrs,
			MessageScheduledForKey.String(env.ScheduledFor.Format(time.RFC3339)),
		)
	}

	if !env.Source.Handler.IsZero() {
		attrs = append(
			attrs,
			MessageSourceHandlerName.String(env.Source.Handler.Name),
			MessageSourceHandlerKey.String(env.Source.Handler.Key),
		)
	}

	if env.Source.InstanceID != "" {
		attrs = append(
			attrs,
			MessageSourceInstanceID.String(env.Source.InstanceID),
		)
	}

	return func(s trace.Span) {
		s.SetAttributes(attrs...)
	}
}
