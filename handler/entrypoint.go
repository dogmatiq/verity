package handler

import (
	"context"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
)

// EntryPoint sets up a unit-of-work for each message to be handled, dispatches
// to a handler, and persists the result.
type EntryPoint struct {
	// QueueEvents is the set of event types that must be added to the queue.
	QueueEvents message.TypeCollection

	// Handler is the handler implmentation that populates the unit-of-work.
	Handler Handler

	// Observers is a set of observers that is added to every unit-of-work.
	Observers []Observer
}

// Acknowledger is an interface for acknowledging handled messages.
type Acknowledger interface {
	// Ack acknowledges the message, ensuring it is not handled again.
	//
	// b is the batch from the unit-of-work.
	Ack(ctx context.Context, b persistence.Batch) (persistence.Result, error)

	// Nack negatively-acknowledges the message, causing it to be retried.
	Nack(ctx context.Context, cause error) error
}

// HandleMessage handles the message in p using ep.Handler and persists the
// result of its unit-of-work.
//
// b is a batch of persistence operations that must be performed atomically with
// the unit-of-work.
func (ep *EntryPoint) HandleMessage(
	ctx context.Context,
	a Acknowledger,
	p parcel.Parcel,
) error {
	// Setup a new unit-of-work. We copy the observers so that we don't mess
	// with the underlying array of ep.Observers as we append new elements while
	// handling the message.
	w := &UnitOfWork{
		queueEvents: ep.QueueEvents,
		observers:   append([]Observer(nil), ep.Observers...),
	}

	// Dispatch the the handler.
	if err := ep.Handler.HandleMessage(ctx, w, p); err != nil {
		w.notifyObservers(err)
		return a.Nack(ctx, err)
	}

	// Perform the combined operations of the unit-of-work and b.
	pr, err := a.Ack(ctx, w.batch)
	if err != nil {
		w.notifyObservers(err)
		return a.Nack(ctx, err)
	}

	// Update the unit-of-work's result to include the offsets from the
	// persistence result.
	w.populateEventOffsets(pr)
	w.notifyObservers(nil)

	return nil
}
