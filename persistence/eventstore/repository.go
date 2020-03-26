package eventstore

import (
	"context"
)

// Repository is an interface for reading persisted event messages.
type Repository interface {
	// QueryEvents queries events in the repository.
	QueryEvents(ctx context.Context, q Query) (Result, error)
}

// Query defines criteria for matching events in the store.
type Query struct {
	// Begin specifies the (inclusive) lower-bound of the offset range to
	// include in the results.
	Begin Offset

	// End specifies the (exclusive) upper-bound of the offset range to to
	// include in the results. If it is 0, the number of stored events is used.
	End Offset

	// PortableNames is the set of event types to include in the results,
	// specified using the "portable type name".
	//
	// If it is empty, all event types are included.
	PortableNames map[string]struct{}

	// AggregateHandlerKey, if non-empty, limits the results to those events
	// produced by the aggregate message handler identified by this key.
	AggregateHandlerKey string

	// AggregateInstanceID, if non-empty, limits the results to those events
	// produced by this aggregate instance.
	AggregateInstanceID string
}

// Result is the result of a query to the event store.
//
// Results are not safe for concurrent use.
type Result interface {
	// Next returns the next event in the result.
	//
	// It returns false if the are no more events in the result.
	Next(ctx context.Context) (*Event, bool, error)

	// Close closes the cursor.
	Close() error
}
