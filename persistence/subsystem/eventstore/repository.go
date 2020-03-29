package eventstore

import (
	"context"
)

// Repository is an interface for reading persisted event messages.
type Repository interface {
	// QueryEvents queries events in the repository.
	QueryEvents(ctx context.Context, q Query) (Result, error)
}

// Filter is a set of portable type names of event messages.
type Filter map[string]struct{}

// NewFilter returns a new filter containing the given names.
func NewFilter(names ...string) Filter {
	f := make(Filter, len(names))

	for _, n := range names {
		f[n] = struct{}{}
	}

	return f
}

// Add adds a name to the filter.
func (f *Filter) Add(n string) {
	if *f == nil {
		*f = Filter{}
	}

	(*f)[n] = struct{}{}
}

// Remove removes a name from the filter.
func (f Filter) Remove(n string) {
	delete(f, n)
}

// Query defines criteria for matching events in the store.
type Query struct {
	// MinOffset specifies the (inclusive) lower-bound of the offset range to
	// include in the results.
	MinOffset Offset

	// Filter is the set of event types to include in the results, specified
	// using the "portable type name".
	//
	// If it is empty, all event types are included.
	Filter Filter

	// AggregateHandlerKey, if non-empty, limits the results to those events
	// produced by the aggregate message handler identified by this key.
	//
	// If it is non-empty AggregateInstanceID must also be non-empty.
	AggregateHandlerKey string

	// AggregateInstanceID limits the results to those events produced by this
	// aggregate instance.
	//
	// It is only used if AggregateHandlerKey is non-empty.
	AggregateInstanceID string
}

// IsMatch returns true if ev matches the query criteria.
func (q Query) IsMatch(ev *Event) bool {
	if ev.Offset < q.MinOffset {
		return false
	}

	if len(q.Filter) > 0 {
		if _, ok := q.Filter[ev.Envelope.PortableName]; !ok {
			return false
		}
	}

	if q.AggregateHandlerKey != "" {
		if ev.Envelope.MetaData.Source.Handler.Key != q.AggregateHandlerKey {
			return false
		}

		if ev.Envelope.MetaData.Source.InstanceId != q.AggregateInstanceID {
			return false
		}
	}

	return true
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
