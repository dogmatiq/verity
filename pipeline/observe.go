package pipeline

import (
	"context"
)

// QueueObserver is a function that is notified when messages are enqueued.
type QueueObserver func(context.Context, []EnqueuedMessage) error

// WhenMessageEnqueued returns a pipeline stage that calls fn when messages
// are enqueued by any subsequent pipeline stage, and that stage is successful.
func WhenMessageEnqueued(fn QueueObserver) Stage {
	return func(ctx context.Context, req Request, res *Response, next Sink) error {
		if err := next(ctx, req, res); err != nil {
			return err
		}

		if len(res.EnqueuedMessages) > 0 {
			return fn(ctx, res.EnqueuedMessages)
		}

		return nil
	}
}

// EventStreamObserver is a function that is notified when events are recorded.
type EventStreamObserver func(context.Context, []RecordedEvent) error

// WhenEventRecorded returns a pipeline stage that calls fn when events are
// recorded by any subsequent pipeline stage, and that state is successful.
func WhenEventRecorded(fn EventStreamObserver) Stage {
	return func(ctx context.Context, req Request, res *Response, next Sink) error {
		if err := next(ctx, req, res); err != nil {
			return err
		}

		if len(res.RecordedEvents) > 0 {
			return fn(ctx, res.RecordedEvents)
		}

		return nil
	}
}
