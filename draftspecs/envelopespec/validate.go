package envelopespec

import (
	"errors"
	"fmt"
)

// CheckWellFormed returns an error if env is not well-formed.
//
// Well-formedness means that all compulsory fields are populated, and that no
// incompatible fields are populated.
//
// It is intentially fairly permissive, so that message meta-data can be
// obtained even if the message is unable to be handled.
//
// It does not perform "deep" validation, such as ensuring messages, times, etc
// can be unmarshaled.
func CheckWellFormed(env *Envelope) error {
	if env.GetMessageId() == "" {
		return errors.New("message ID must not be empty")
	}

	if env.GetCausationId() == "" {
		return errors.New("causation ID must not be empty")
	}

	if env.GetCorrelationId() == "" {
		return errors.New("correlation ID must not be empty")
	}

	if err := checkIdentity(env.GetSourceApplication()); err != nil {
		return fmt.Errorf("application identity is invalid: %w", err)
	}

	h := env.GetSourceHandler()

	if isEmpty(h) {
		if env.GetSourceInstanceId() != "" {
			return errors.New("source instance ID must not be specified without providing a handler identity")
		}
	} else if err := checkIdentity(h); err != nil {
		return fmt.Errorf("handler identity is invalid: %w", err)
	}

	if env.GetCreatedAt() == "" {
		return errors.New("created-at time must not be empty")
	}

	if env.GetScheduledFor() != "" && env.GetSourceInstanceId() == "" {
		return errors.New("scheduled-for time must not be specified without a providing source handler and instance ID")
	}

	// Note: we allow md.Description to be empty. Some messages may simply not
	// have a concise human-readable description available.

	if env.GetPortableName() == "" {
		return errors.New("portable name must not be empty")
	}

	if env.GetMediaType() == "" {
		return errors.New("media-type must not be empty")
	}

	// Note, env.Data *may* be empty. A specific application's marshaler could
	// conceivably have a message with no data where the message type is
	// sufficient information.

	return nil
}

// MustBeWellFormed panics if env is not well-formed.
func MustBeWellFormed(env *Envelope) {
	if err := CheckWellFormed(env); err != nil {
		panic(err)
	}
}

// checkIdentity returns an error if id is not well-formed.
func checkIdentity(id *Identity) error {
	if id.GetName() == "" {
		return errors.New("identity name must not be empty")
	}

	if id.GetKey() == "" {
		return errors.New("identity key must not be empty")
	}

	return nil
}

// isEmpty returns true if the given id is empty.
func isEmpty(id *Identity) bool {
	return id.GetName() == "" && id.GetKey() == ""
}
