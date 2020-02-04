package tracing

import (
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"go.opentelemetry.io/otel/api/key"
)

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
	// MessageSourceApplication is a span attribute key for a message's source
	// application identity.
	MessageSourceApplication = key.New("dogma.message.source.application")

	// MessageSourceHandler is a span attribute key for a message's source
	// handler identity.
	MessageSourceHandler = key.New("dogma.message.source.handler")

	// MessageSourceInstanceID is a span attribute key for a message's source
	// instance (for aggregates and processes).
	MessageSourceInstanceID = key.New("dogma.message.source.instance_id")
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
