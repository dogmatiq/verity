package pipeline

import (
	"context"

	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// EnqueuedMessage contains a message that was enqueued via a scope.
type EnqueuedMessage struct {
	Parcel    *parcel.Parcel
	Persisted *queuestore.Item
}

// QueueObserver is a function that is notified when messages are enqueued.
type QueueObserver func(context.Context, []EnqueuedMessage) error

// WhenMessageEnqueued returns a pipeline stage that calls fn when messages
// are enqueued by any subsequent pipeline stage, and that stage is successful.
func WhenMessageEnqueued(fn QueueObserver) Stage {
	return func(ctx context.Context, sc *Scope, next Sink) error {
		if err := next(ctx, sc); err != nil {
			return err
		}

		if len(sc.Enqueued) > 0 {
			return fn(ctx, sc.Enqueued)
		}

		return nil
	}
}

// RecordedEvent contains an event that was recorded via a scope.
type RecordedEvent struct {
	Parcel    *parcel.Parcel
	Persisted *eventstore.Item
}

// EventStreamObserver is a function that is notified when events are recorded.
type EventStreamObserver func(context.Context, []RecordedEvent) error

// WhenEventRecorded returns a pipeline stage that calls fn when events are
// recorded by any subsequent pipeline stage, and that state is successful.
func WhenEventRecorded(fn EventStreamObserver) Stage {
	return func(ctx context.Context, sc *Scope, next Sink) error {
		if err := next(ctx, sc); err != nil {
			return err
		}

		if len(sc.Recorded) > 0 {
			return fn(ctx, sc.Recorded)
		}

		return nil
	}
}
