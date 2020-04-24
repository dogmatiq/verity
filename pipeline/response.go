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
	queueParcels []*parcel.Parcel
	queueItems   []*queuestore.Item
	eventParcels []*parcel.Parcel
	eventItems   []*eventstore.Item
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

	r.queueParcels = append(r.queueParcels, p)
	r.queueItems = append(r.queueItems, i)

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

	i := &eventstore.Item{
		Offset:   o,
		Envelope: p.Envelope,
	}

	r.eventParcels = append(r.eventParcels, p)
	r.eventItems = append(r.eventItems, i)

	return o, nil
}
