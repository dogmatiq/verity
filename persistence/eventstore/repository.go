package eventstore

import (
	"context"

	"github.com/dogmatiq/configkit/message"
)

// Repository is an interface for reading persisted event messages.
type Repository interface {
	// QueryEvents queries events in the repository.
	//
	// types is the set of event types that should be returned by Cursor.Next().
	// Any other event types are ignored.
	QueryEvents(ctx context.Context, q Query) (ResultSet, error)
}

// Query defines criteria for matching events in the store.
type Query struct {
	// NotBefore specifies the lowest offset to include in the results.
	NotBefore Offset

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

// ResultSet is the result of a query to the event store.
type ResultSet interface {
	// Next advances to the next event in the result set.
	//
	// It returns false if the end of the result set is reached.
	Next() bool

	// Next returns current event in the result set.
	Get(ctx context.Context) (*Event, error)

	// Close closes the cursor.
	Close() error
}
