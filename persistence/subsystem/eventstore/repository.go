package eventstore

import (
	"context"
	"fmt"
)

// UnknownMessageError is the error returned  by Repository.LoadEventsBySource()
// method if the barrier message can not be found.
type UnknownMessageError struct {
	MessageID string
}

// Error returns a string representation of UnknownMessageError.
func (e UnknownMessageError) Error() string {
	return fmt.Sprintf(
		"message with ID '%s' cannot be found",
		e.MessageID,
	)
}

// Repository is an interface for reading persisted event messages.
type Repository interface {
	// NextEventOffset returns the next "unused" offset within the store.
	NextEventOffset(ctx context.Context) (uint64, error)

	// LoadEventsByType loads events that match a specific set of message types.
	//
	// f is the set of message types to include in the result. The keys of f are
	// the "portable type name" produced when the events are marshaled.
	//
	// o specifies the (inclusive) lower-bound of the offset range to include in
	// the results.
	LoadEventsByType(
		ctx context.Context,
		f map[string]struct{},
		o uint64,
	) (Result, error)

	// LoadEventsBySource loads the events produced by a specific handler.
	//
	// hk is the handler's identity key.
	//
	// id is the instance ID, which must be empty if the handler type does not
	// use instances.
	//
	// m is ID of a "barrier" message. If supplied, the results are limited to
	// events with higher offsets than the barrier message. If the message
	// cannot be found, UnknownMessageError is returned.
	LoadEventsBySource(
		ctx context.Context,
		hk, id, m string,
	) (Result, error)
}

// Result is the result of a query to the event store.
//
// Results are not safe for concurrent use.
type Result interface {
	// Next returns the next event in the result.
	//
	// It returns false if the are no more events in the result.
	Next(ctx context.Context) (*Item, bool, error)

	// Close closes the cursor.
	Close() error
}
