package message

import (
	"errors"
	"time"

	"github.com/dogmatiq/configkit"
)

// MetaData is a container for meta-data about a message.
type MetaData struct {
	// MessageID is a unique identifier for the message.
	MessageID string

	// CausationID is the ID of the message that was being handled when the message
	// identified by MessageID was produced.
	CausationID string

	// CorrelationID is the ID of the "root" message that entered the application
	// to cause the message identified by MessageID, either directly or indirectly.
	CorrelationID string

	// SourceApp is the identity of the Dogma application that produced this
	// message.
	SourceApp configkit.Identity

	// SourceOffset is the offset of an event within its application's event
	// store. It is nil if the message in this envelope is not an event, or it
	// has not yet been written to the store.
	SourceOffset *uint64

	// SourceHandler is the identity of the handler that produced the message.
	// It is the zero-value if the message was not produced by a handler.
	SourceHandler configkit.Identity

	// SourceInstanceID is the aggregate or process instance that produced the
	// message. It is empty if the message was not produced by a handler, or it
	// was produced by an integration handler.
	SourceInstanceID string

	// CreatedAt is the time at which the message was created.
	CreatedAt time.Time

	// ScheduledFor is the time at which a timeout message was scheduled.
	// The value is undefined if the message is not a timeout.
	ScheduledFor time.Time
}

// Validate returns an error if md is invalid.
func (md *MetaData) Validate() error {
	if md.MessageID == "" {
		return errors.New("message ID must not be empty")
	}

	if md.CausationID == "" {
		return errors.New("causation ID must not be empty")
	}

	if md.CorrelationID == "" {
		return errors.New("correlation ID must not be empty")
	}

	if md.SourceApp.IsZero() {
		return errors.New("source app name must not be empty")
	}

	if md.SourceInstanceID != "" && md.SourceHandler.IsZero() {
		return errors.New("source handler name must not be empty when source instance ID is present")
	}

	if md.CreatedAt.IsZero() {
		return errors.New("created-at time must not be zero")
	}

	return nil
}
