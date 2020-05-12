package handler

import (
	"context"
	"time"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/dogmatiq/infix/queue"
)

// Persist saves the state changes in w and notifies observers of the result.
func Persist(
	ctx context.Context,
	p persistence.Persister,
	w *UnitOfWork,
) error {
	res, err := p.Persist(ctx, w.batch)

	// Populate the events in the result with their offsets.
	//
	// Note: we just iterate through the events in the result to find each match
	// rather than maintaining a map or any other more elaborate data structure.
	// This is because we expect the vast majority of results to contain 0 or 1
	// event.
	for _, item := range res.EventStoreItems {
		for i := range w.result.Events {
			ev := &w.result.Events[i]

			if ev.Parcel.Envelope.MetaData.MessageId == item.ID() {
				ev.Offset = item.Offset
			}
		}
	}

	// Notify the observers.
	for _, o := range w.observers {
		o(w.result, err)
	}

	return err
}

// UnitOfWork encapsulates the state changes made by one or more handlers in the
// process of handling a single message.
type UnitOfWork struct {
	// QueueEvents is the set of event types that should be added to the queue.
	QueueEvents message.TypeCollection

	result    Result
	batch     persistence.Batch
	observers []Observer
}

// ExecuteCommand enqueues a command for execution.
func (w *UnitOfWork) ExecuteCommand(p *parcel.Parcel) {
	w.saveQueueItem(p, p.CreatedAt)
}

// ScheduleTimeout schedules a timeout message for execution.
func (w *UnitOfWork) ScheduleTimeout(p *parcel.Parcel) {
	w.saveQueueItem(p, p.ScheduledFor)
}

// RecordEvent records an event message.
func (w *UnitOfWork) RecordEvent(p *parcel.Parcel) {
	w.saveEvent(p)

	if w.QueueEvents != nil && w.QueueEvents.HasM(p.Message) {
		w.saveQueueItem(p, p.CreatedAt)
	}
}

// Do adds an arbitrary persistence operation to the unit-of-work.
func (w *UnitOfWork) Do(op persistence.Operation) {
	w.batch = append(w.batch, op)
}

// Observe registers a function to be notified when the unit-of-work is
// complete.
func (w *UnitOfWork) Observe(o Observer) {
	w.observers = append(w.observers, o)
}

// saveEvent adds a SaveEvent operation to the batch for the message in p.
func (w *UnitOfWork) saveEvent(p *parcel.Parcel) {
	w.batch = append(
		w.batch,
		persistence.SaveEvent{
			Envelope: p.Envelope,
		},
	)

	w.result.Events = append(
		w.result.Events,
		eventstream.Event{
			Offset: 0, // unknown until persisted
			Parcel: p,
		},
	)
}

// saveEvent adds a SaveQueueItem operation to the batch for the message in p.
func (w *UnitOfWork) saveQueueItem(p *parcel.Parcel, next time.Time) {
	item := queuestore.Item{
		NextAttemptAt: next,
		Envelope:      p.Envelope,
	}

	w.batch = append(
		w.batch,
		persistence.SaveQueueItem{
			Item: item,
		},
	)

	item.Revision++

	w.result.Queued = append(
		w.result.Queued,
		queue.Message{
			Parcel: p,
			Item:   &item,
		},
	)
}
