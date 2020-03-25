package eventstore

import (
	"context"

	"github.com/dogmatiq/configkit/message"
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

	// Types is the set of event types to include in the results.
	// If it is nil, all event types are included.
	Types message.TypeCollection

	// AggregateHandlerKey, if non-empty, limits the results to those events
	// produced by the aggregate message handler identified by this key.
	AggregateHandlerKey string

	// AggregateInstanceID, if non-empty, limits the results to those events
	// produced by this aggregate instance.
	AggregateInstanceID string
}

// Result is the result of a query to the event store.
type Result interface {
	// Next advances to the next event in the result.
	//
	// It returns false if the are no more events in the result.
	Next() bool

	// Next returns current event in the result.
	Get(ctx context.Context) (*Event, error)

	// Close closes the cursor.
	Close() error
}
