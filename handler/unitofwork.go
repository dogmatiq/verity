package handler

import (
	"time"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/verity/eventstream"
	"github.com/dogmatiq/verity/parcel"
	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/queue"
)

// UnitOfWork encapsulates the state changes made by one or more handlers in the
// process of handling a single message.
type UnitOfWork interface {
	// ExecuteCommand updates the unit-of-work to execute the command in p.
	ExecuteCommand(p parcel.Parcel)

	// ScheduleTimeout updates the unit-of-work to schedule the timeout in p.
	ScheduleTimeout(p parcel.Parcel)

	// RecordEvent updates the unit-of-work to record the event in p.
	RecordEvent(p parcel.Parcel)

	// Do updates the unit-of-work to include op in the persistence batch.
	Do(op persistence.Operation)

	// Defer registers fn to be called when the unit-of-work is complete.
	//
	// Like Go's defer keyword, deferred functions guaranteed to be invoked in
	// the reverse order to which they are registered.
	Defer(fn DeferFunc)
}

// Result is the result of a successful unit-of-work.
type Result struct {
	// Queued is the set of messages that were placed on the message queue,
	// which may include events.
	Queued []queue.Message

	// Events is the set of events that were recorded in the unit-of-work.
	Events []eventstream.Event
}

// DeferFunc is a function can be deferred until a unit-of-work is completed.
type DeferFunc func(Result, error)

// unitOfWork is the implementation of UnitOfWork used by an EntryPoint.
type unitOfWork struct {
	queueEvents message.TypeCollection
	batch       persistence.Batch
	result      Result
	deferred    []DeferFunc
}

// ExecuteCommand updates the unit-of-work to execute the command in p.
func (w *unitOfWork) ExecuteCommand(p parcel.Parcel) {
	w.saveQueueMessage(p, p.CreatedAt)
}

// ScheduleTimeout updates the unit-of-work to schedule the timeout in p.
func (w *unitOfWork) ScheduleTimeout(p parcel.Parcel) {
	w.saveQueueMessage(p, p.ScheduledFor)
}

// RecordEvent updates the unit-of-work to record the event in p.
func (w *unitOfWork) RecordEvent(p parcel.Parcel) {
	w.saveEvent(p)

	if w.queueEvents != nil && w.queueEvents.HasM(p.Message) {
		w.saveQueueMessage(p, p.CreatedAt)
	}
}

// Do updates the unit-of-work to include op in the persistence batch.
func (w *unitOfWork) Do(op persistence.Operation) {
	w.batch = append(w.batch, op)
}

// Defer registers fn to be called when the unit-of-work is complete.
//
// Like Go's defer keyword, deferred functions guaranteed to be invoked in
// the reverse order to which they are registered.
func (w *unitOfWork) Defer(fn DeferFunc) {
	w.deferred = append(w.deferred, fn)
}

// saveQueueMessage adds a SaveQueueMessage operation to the batch for p.
func (w *unitOfWork) saveQueueMessage(p parcel.Parcel, next time.Time) {
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
func (w *unitOfWork) saveEvent(p parcel.Parcel) {
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
func (w *unitOfWork) populateEventOffsets(pr persistence.Result) {
	for i := range w.result.Events {
		ev := &w.result.Events[i]
		ev.Offset = pr.EventOffsets[ev.ID()]
	}
}

// invokeDeferred calls each deferred function in the unit-of-work.
func (w *unitOfWork) invokeDeferred(err error) {
	for i := len(w.deferred) - 1; i >= 0; i-- {
		w.deferred[i](w.result, err)
	}
}
