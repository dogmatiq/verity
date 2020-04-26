package pipeline

import (
	"context"

	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// A Sink accepts a pipeline request and populates the response.
type Sink func(context.Context, Request, *Response) error

// Stage is a segment of a pipeline.
type Stage func(context.Context, Request, *Response, Sink) error

// Pipeline is a message processing pipeline.
type Pipeline func(context.Context, Request) error

// QueueObserver is a function that is notified when messages are enqueued.
//
// The observer takes ownership of the given slices, they must not be used after
// notifying the observer.
type QueueObserver func(
	context.Context,
	[]*parcel.Parcel,
	[]*queuestore.Item,
) error

// EventObserver is a function that is notified when events are recorded.
//
// The observer takes ownership of the given slices, they must not be used after
// notifying the observer.
type EventObserver func(
	context.Context,
	[]*parcel.Parcel,
	[]*eventstore.Item,
) error

// New returns a Port that notifies observers when a request results in
// messages being enqueued or events being recorded.
func New(
	q QueueObserver,
	e EventObserver,
	next Sink,
) Pipeline {
	return func(ctx context.Context, req Request) error {
		res := &Response{}

		if err := next(ctx, req, res); err != nil {
			return err
		}

		if q != nil && len(res.queueParcels) > 0 {
			if err := q(ctx, res.queueParcels, res.queueItems); err != nil {
				return err
			}
		}

		if e != nil && len(res.eventParcels) > 0 {
			if err := e(ctx, res.eventParcels, res.eventItems); err != nil {
				return err
			}
		}

		return nil
	}
}
