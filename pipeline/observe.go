package pipeline

import (
	"context"

	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// QueueObserver is a function that is notified when messages are enqueued.
type QueueObserver func(
	context.Context,
	[]*parcel.Parcel,
	[]*queuestore.Item,
) error

// WhenMessageEnqueued returns a pipeline stage that calls fn when messages
// are enqueued by any subsequent pipeline stage, and that stage is successful.
func WhenMessageEnqueued(fn QueueObserver) Stage {
	return func(ctx context.Context, req Request, res *Response, next Sink) error {
		if err := next(ctx, req, res); err != nil {
			return err
		}

		if len(res.queueParcels) > 0 {
			return fn(ctx, res.queueParcels, res.queueItems)
		}

		return nil
	}
}

// EventObserver is a function that is notified when events are recorded.
type EventObserver func(
	context.Context,
	[]*parcel.Parcel,
	[]*eventstore.Item,
) error

// WhenEventRecorded returns a pipeline stage that calls fn when events are
// recorded by any subsequent pipeline stage, and that state is successful.
func WhenEventRecorded(fn EventObserver) Stage {
	return func(ctx context.Context, req Request, res *Response, next Sink) error {
		if err := next(ctx, req, res); err != nil {
			return err
		}

		if len(res.eventParcels) > 0 {
			return fn(ctx, res.eventParcels, res.eventItems)
		}

		return nil
	}
}
