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

// RecordedEvent contains an event that was recorded via a scope.
type RecordedEvent struct {
	Memory    *envelope.Envelope
	Persisted *eventstore.Event
}

// WhenMessageEnqueued returns a pipeline stage that calls fn() when messages
// are enqueued by any subsequent pipeline stage, and that stage is successful.
func WhenMessageEnqueued(
	fn func(context.Context, []EnqueuedMessage) error,
) Stage {
	return func(ctx context.Context, sc *Scope, next Sink) error {
		if err := next(ctx, sc); err != nil {
			return err
		}

		if len(sc.enqueued) > 0 {
			return fn(ctx, sc.enqueued)
		}

		return nil
	}
}

// WhenEventRecorded returns a pipeline stage that calls fn() when events are
// recorded by any subsequent pipeline stage, and that state is successful.
func WhenEventRecorded(
	fn func(context.Context, []RecordedEvent) error,
) Stage {
	return func(ctx context.Context, sc *Scope, next Sink) error {
		if err := next(ctx, sc); err != nil {
			return err
		}

		if len(sc.recorded) > 0 {
			return fn(ctx, sc.recorded)
		}

		return nil
	}
}
