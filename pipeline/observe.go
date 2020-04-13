package pipeline

import (
	"context"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// EnqueuedMessage contains a message that was enqueued via a scope.
type EnqueuedMessage struct {
	Memory    *envelope.Envelope
	Persisted *queuestore.Message
}

// EnqueuedMessageObserver is a function that is notified when messages are
// enqueued.
type EnqueuedMessageObserver func(context.Context, []EnqueuedMessage) error

// WhenMessageEnqueued returns a pipeline stage that calls fn when messages
// are enqueued by any subsequent pipeline stage, and that stage is successful.
func WhenMessageEnqueued(fn EnqueuedMessageObserver) Stage {
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
	Memory *envelope.Envelope
	Parcel *eventstore.Parcel
}

// RecordedEventObserver is a function that is notified when events are
// recorded.
type RecordedEventObserver func(context.Context, []RecordedEvent) error

// WhenEventRecorded returns a pipeline stage that calls fn when events are
// recorded by any subsequent pipeline stage, and that state is successful.
func WhenEventRecorded(fn RecordedEventObserver) Stage {
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
