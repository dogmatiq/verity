package handler

import (
	"time"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/dogmatiq/infix/queue"
)

// UnitOfWork encapsulates the state changes made by one or more handlers in the
// process of handling a single message.
type UnitOfWork struct {
	// Commands contains parcels for command messages produced by the handler.
	Commands []*parcel.Parcel

	// Timeouts contains parcels for timeout messages produced by the handler.
	Timeouts []*parcel.Parcel

	// Events contains parcels for event messages produced by the handler.
	Events []*parcel.Parcel

	// Batch contains arbitrary persistence operations required by the handler.
	Batch persistence.Batch

	// Observers contains observers to notify when the unit-of-work is complete.
	Observers []Observer
}

// Resolver resolves the persistence batch and final result of a
// unit-of-work.
type Resolver struct {
	// QueueEvents contains the event types that must be added to the queue.
	QueueEvents message.TypeCollection

	// UnitOfWork is the unit-of-work to resolve.
	UnitOfWork *UnitOfWork

	batch  persistence.Batch
	result Result
}

// ResolveBatch returns the persistence batch that must be performed to persist
// the unit-of-work.
//
// qe is the set of event types that must be added to the message queue.
func (r *Resolver) ResolveBatch() persistence.Batch {
	for _, p := range r.UnitOfWork.Commands {
		r.saveQueueItem(p, p.CreatedAt)

	}

	for _, p := range r.UnitOfWork.Timeouts {
		r.saveQueueItem(p, p.ScheduledFor)
	}

	for _, p := range r.UnitOfWork.Events {
		if r.QueueEvents.HasM(p.Message) {
			r.saveQueueItem(p, p.CreatedAt)
		}

		r.saveEvent(p)
	}

	r.batch = append(r.batch, r.UnitOfWork.Batch...)

	return r.batch
}

// ResolveResult returns the result of the unit-of-work.
//
// pr is the result of persisting the batch returned by r.ResolveBatch().
func (r *Resolver) ResolveResult(pr persistence.Result) Result {
	// Populate the events in the result with their offsets.
	//
	// Note: we just iterate through the events in the result to find each match
	// rather than maintaining a map or any other more elaborate data structure.
	// This is because we expect the vast majority of results to contain 0 or 1
	// event.
	for _, item := range pr.EventStoreItems {
		for i := range r.result.Events {
			ev := &r.result.Events[i]

			if ev.Parcel.Envelope.MetaData.MessageId == item.ID() {
				ev.Offset = item.Offset
			}
		}
	}

	return r.result
}

// saveQueueItem adds a SaveQueueItem operation to the batch for p.
func (r *Resolver) saveQueueItem(p *parcel.Parcel, next time.Time) {
	item := queuestore.Item{
		NextAttemptAt: next,
		Envelope:      p.Envelope,
	}

	r.batch = append(
		r.batch,
		persistence.SaveQueueItem{
			Item: item,
		},
	)

	item.Revision++

	r.result.Queued = append(
		r.result.Queued,
		queue.Message{
			Parcel: p,
			Item:   &item,
		},
	)
}

// saveEvent adds a SaveEvent operation to the batch for p.
func (r *Resolver) saveEvent(p *parcel.Parcel) {
	r.batch = append(
		r.batch,
		persistence.SaveEvent{
			Envelope: p.Envelope,
		},
	)

	r.result.Events = append(
		r.result.Events,
		eventstream.Event{
			Offset: 0, // unknown until persistence is complete
			Parcel: p,
		},
	)
}

// Result is the result of a successful unit-of-work.
type Result struct {
	// Queued is the set of messages that were placed on the message queue,
	// which may include events.
	Queued []queue.Message

	// Events is the set of events that were recorded in the unit-of-work.
	Events []eventstream.Event
}

// Observer is a function that is notified of the result of a unit-of-work.
type Observer func(Result, error)

// NotifyObservers notifies the observers in w.
func NotifyObservers(w *UnitOfWork, res Result, err error) {
	for _, obs := range w.Observers {
		obs(res, err)
	}
}
