package pipeline

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// Response is the result from a pipeline stage.
//
// It encapsulates the messages that were produced, so they may be observed by
// other components of the engine.
type Response struct {
	// EnqueuedMessages contains the messages that were enqueued during handling
	// of the request.
	EnqueuedMessages []EnqueuedMessage

	// RecordedEvents contains the events that were recorded during handling of
	// the request.
	RecordedEvents []RecordedEvent
}

// EnqueuedMessage contains a message that was enqueued via a scope.
type EnqueuedMessage struct {
	Parcel    *parcel.Parcel
	Persisted *queuestore.Item
}

// RecordedEvent contains an event that was recorded via a scope.
type RecordedEvent struct {
	Parcel    *parcel.Parcel
	Persisted *eventstore.Item
}

// EnqueueMessage is a helper method that adds a message to the queue and
// adds it to the response.
func (r *Response) EnqueueMessage(
	ctx context.Context,
	tx persistence.ManagedTransaction,
	p *parcel.Parcel,
) error {
	n := p.ScheduledFor
	if n.IsZero() {
		n = time.Now()
	}

	i := &queuestore.Item{
		NextAttemptAt: n,
		Envelope:      p.Envelope,
	}

	if err := tx.SaveMessageToQueue(ctx, i); err != nil {
		return err
	}

	i.Revision++

	r.EnqueuedMessages = append(
		r.EnqueuedMessages,
		EnqueuedMessage{
			Parcel:    p,
			Persisted: i,
		},
	)

	return nil
}

// RecordEvent is a helper method that appends an event to the event stream and
// adds it to the response.
func (r *Response) RecordEvent(
	ctx context.Context,
	tx persistence.ManagedTransaction,
	p *parcel.Parcel,
) (eventstore.Offset, error) {
	o, err := tx.SaveEvent(ctx, p.Envelope)
	if err != nil {
		return 0, err
	}

	r.RecordedEvents = append(
		r.RecordedEvents,
		RecordedEvent{
			Parcel: p,
			Persisted: &eventstore.Item{
				Offset:   o,
				Envelope: p.Envelope,
			},
		},
	)

	return o, nil
}
