package pipeline

import (
	"context"

	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// QueueObserver is a function that is notified when messages are
// enqueued.
type QueueObserver func(context.Context, []queuestore.Pair) error

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

// EventStreamObserver is a function that is notified when events are
// recorded.
type EventStreamObserver func(context.Context, []eventstore.Pair) error

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
