package persistence

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// Event is a persisted event message.
type Event struct {
	Offset   uint64
	Envelope *envelopepb.Envelope
}

// ID returns the ID of the message.
func (e Event) ID() string {
	return e.Envelope.GetMessageId().AsString()
}

// EventRepository is an interface for reading event messages.
type EventRepository interface {
	// NextEventOffset returns the next "unused" offset.
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
	) (EventResult, error)

	// LoadEventsBySource loads the events produced by a specific handler.
	//
	// hk is the handler's identity key.
	//
	// id is the instance ID, which must be empty if the handler type does not
	// use instances.
	LoadEventsBySource(
		ctx context.Context,
		hk, id string,
	) (EventResult, error)
}

// EventResult is the result of a query made using an EventRepository.
//
// EventResult values are not safe for concurrent use.
type EventResult interface {
	// Next returns the next event in the result.
	//
	// It returns false if the are no more events in the result.
	Next(ctx context.Context) (Event, bool, error)

	// Close closes the cursor.
	Close() error
}

// SaveEvent is an Operation that persists an event message.
type SaveEvent struct {
	// Envelope is the envelope containing the event to persist.
	Envelope *envelopepb.Envelope
}

// AcceptVisitor calls v.VisitSaveEvent().
func (op SaveEvent) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitSaveEvent(ctx, op)
}

func (op SaveEvent) entityKey() entityKey {
	return entityKey{"event", op.Envelope.GetMessageId().AsString()}
}
