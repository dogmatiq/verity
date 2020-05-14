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

	// Persister is used to persist the unit-of-work.
	Persister persistence.Persister

	// Handler is the handler implmentation that populates the unit-of-work.
	Handler Handler

	// Observers is a set of observers that is added to every unit-of-work.
	Observers []Observer
}

// HandleMessage handles the message in p using x.Handler and persists the
// result of its unit-of-work.
//
// b is a batch of persistence operations that must be performed atomically with
// the unit-of-work.
func (ep *EntryPoint) HandleMessage(
	ctx context.Context,
	p *parcel.Parcel,
	b persistence.Batch,
) (err error) {
	// Setup a new unit-of-work. We copy the observers so that we don't mess
	// with the underlying array of x.Observers as we append new elements while
	// handling the message.
	w := &UnitOfWork{
		queueEvents: ep.QueueEvents,
		observers:   append([]Observer(nil), ep.Observers...),
	}

	// Ensure we always notify the observers, regardless of the result.
	defer func() {
		w.notifyObservers(err)
	}()

	// Dispatch the the handler.
	if err := ep.Handler.HandleMessage(ctx, w, p); err != nil {
		return err
	}

	// Perform the combined operations of the unit-of-work and b.
	batch := append(w.batch, b...)
	pr, err := ep.Persister.Persist(ctx, batch)
	if err != nil {
		return err
	}

	// Update the unit-of-work's result to include the offsets from the
	// persistence result.
	w.populateEventOffsets(pr)

	return nil
}
