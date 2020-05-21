package handler

import (
	"time"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/queue"
)

// UnitOfWork encapsulates the state changes made by one or more handlers in the
// process of handling a single message.
type UnitOfWork struct {
	queueEvents message.TypeCollection
	batch       persistence.Batch
	result      Result
	observers   []Observer
}

// ExecuteCommand updates the unit-of-work to execute the command in p.
func (w *UnitOfWork) ExecuteCommand(p parcel.Parcel) {
	w.saveQueueMessage(p, p.CreatedAt)
}

// ScheduleTimeout updates the unit-of-work to schedule the timeout in p.
func (w *UnitOfWork) ScheduleTimeout(p parcel.Parcel) {
	w.saveQueueMessage(p, p.ScheduledFor)
}

// RecordEvent updates the unit-of-work to record the event in p.
func (w *UnitOfWork) RecordEvent(p parcel.Parcel) {
	w.saveEvent(p)

	if w.queueEvents != nil && w.queueEvents.HasM(p.Message) {
		w.saveQueueMessage(p, p.CreatedAt)
	}
}

// Do updates the unit-of-work to include op in the persistence batch.
func (w *UnitOfWork) Do(op persistence.Operation) {
	w.batch = append(w.batch, op)
}

// Observe adds an observer to be notified when the unit-of-work is complete.
func (w *UnitOfWork) Observe(obs Observer) {
	w.observers = append(w.observers, obs)
}

// saveQueueMessage adds a SaveQueueMessage operation to the batch for p.
func (w *UnitOfWork) saveQueueMessage(p parcel.Parcel, next time.Time) {
	qm := persistence.QueueMessage{
		NextAttemptAt: next,
		Envelope:      p.Envelope,
	}

	w.batch = append(
		w.batch,
		persistence.SaveQueueMessage{
			Message: qm,
		},
	)

	qm.Revision++

	w.result.Queued = append(
		w.result.Queued,
		queue.Message{
			QueueMessage: qm,
			Parcel:       p,
		},
	)
}

// saveEvent adds a SaveEvent operation to the batch for p.
func (w *UnitOfWork) saveEvent(p parcel.Parcel) {
	w.batch = append(
		w.batch,
		persistence.SaveEvent{
			Envelope: p.Envelope,
		},
	)

	w.result.Events = append(
		w.result.Events,
		eventstream.Event{
			Offset: 0, // unknown until persistence is complete
			Parcel: p,
		},
	)
}

// populateEventOffsets updates the events in w.result with their offsets.
func (w *UnitOfWork) populateEventOffsets(pr persistence.Result) {
	for i := range w.result.Events {
		ev := &w.result.Events[i]
		ev.Offset = pr.EventOffsets[ev.Parcel.Envelope.MetaData.MessageId] // TODO: https://github.com/dogmatiq/infix/issues/268
	}
}

// notifyObservers calls each observer in the unit-of-work.
func (w *UnitOfWork) notifyObservers(err error) {
	for _, obs := range w.observers {
		obs(w.result, err)
	}
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
