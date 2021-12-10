package persistence

import (
	"fmt"
)

// UnknownMessageError is the error returned when a message referenced by its ID
// does not exist.
type UnknownMessageError struct {
	MessageID string
}

// Error returns a string representation of UnknownMessageError.
func (e UnknownMessageError) Error() string {
	return fmt.Sprintf(
		"message with ID '%s' does not exist",
		e.MessageID,
	)
}

// ConflictError is an error indicating one or more operations within a batch
// caused an optimistic concurrency conflict.
type ConflictError struct {
	// Cause is the operation that caused the conflict.
	Cause Operation
}

func (e ConflictError) Error() string {
	return fmt.Sprintf(
		"optimistic concurrency conflict in %T operation",
		e.Cause,
	)
}

// NotFoundError is an error indicating one or more operations within a batch
// caused a record not found error.
type NotFoundError struct {
	// Cause is the operation that caused error.
	Cause Operation
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf(
		"record not found in %T operation",
		e.Cause,
	)
}
