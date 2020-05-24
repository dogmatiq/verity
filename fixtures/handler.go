package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
)

// HandlerStub is a test implementation of the handler.Provider interface.
type HandlerStub struct {
	handler.Handler

	HandleMessageFunc func(context.Context, handler.UnitOfWork, parcel.Parcel) error
}

// HandleMessage handles the message in p.
func (h *HandlerStub) HandleMessage(ctx context.Context, w handler.UnitOfWork, p parcel.Parcel) error {
	if h.HandleMessageFunc != nil {
		return h.HandleMessageFunc(ctx, w, p)
	}

	if h.Handler != nil {
		return h.Handler.HandleMessage(ctx, w, p)
	}

	return nil
}

// UnitOfWorkStub is a test implementation of the handler.UnitOfWork interface.
type UnitOfWorkStub struct {
	handler.UnitOfWork

	ExecuteCommandFunc  func(parcel.Parcel)
	ScheduleTimeoutFunc func(parcel.Parcel)
	RecordEventFunc     func(parcel.Parcel)
	DoFunc              func(persistence.Operation)
	ObserveFunc         func(handler.Observer)
}

// ExecuteCommand updates the unit-of-work to execute the command in p.
func (w *UnitOfWorkStub) ExecuteCommand(p parcel.Parcel) {
	if w.ExecuteCommandFunc != nil {
		w.ExecuteCommandFunc(p)
	}

	if w.UnitOfWork != nil {
		w.UnitOfWork.ExecuteCommand(p)
	}
}

// ScheduleTimeout updates the unit-of-work to schedule the timeout in p.
func (w *UnitOfWorkStub) ScheduleTimeout(p parcel.Parcel) {
	if w.ScheduleTimeoutFunc != nil {
		w.ScheduleTimeoutFunc(p)
	}

	if w.UnitOfWork != nil {
		w.UnitOfWork.ScheduleTimeout(p)
	}
}

// RecordEvent updates the unit-of-work to record the event in p.
func (w *UnitOfWorkStub) RecordEvent(p parcel.Parcel) {
	if w.RecordEventFunc != nil {
		w.RecordEventFunc(p)
	}

	if w.UnitOfWork != nil {
		w.UnitOfWork.RecordEvent(p)
	}
}

// Do updates the unit-of-work to include op in the persistence batch.
func (w *UnitOfWorkStub) Do(op persistence.Operation) {
	if w.DoFunc != nil {
		w.DoFunc(op)
	}

	if w.UnitOfWork != nil {
		w.UnitOfWork.Do(op)
	}
}

// Observe adds an observer to be notified when the unit-of-work is complete.
func (w *UnitOfWorkStub) Observe(obs handler.Observer) {
	if w.ObserveFunc != nil {
		w.ObserveFunc(obs)
	}

	if w.UnitOfWork != nil {
		w.UnitOfWork.Observe(obs)
	}
}

// AcknowledgerStub is a test implementation of the handler.Acknowledger
// interface.
type AcknowledgerStub struct {
	handler.Acknowledger

	AckFunc  func(context.Context, persistence.Batch) (persistence.Result, error)
	NackFunc func(context.Context, error) error
}

// Ack acknowledges the message, ensuring it is not handled again.
//
// b is the batch from the unit-of-work.
func (a *AcknowledgerStub) Ack(ctx context.Context, b persistence.Batch) (persistence.Result, error) {
	if a.AckFunc != nil {
		return a.AckFunc(ctx, b)
	}

	if a.Acknowledger != nil {
		return a.Acknowledger.Ack(ctx, b)
	}

	return persistence.Result{}, nil
}

// Nack negatively-acknowledges the message, causing it to be retried.
func (a *AcknowledgerStub) Nack(ctx context.Context, cause error) error {
	if a.NackFunc != nil {
		return a.NackFunc(ctx, cause)
	}

	if a.Acknowledger != nil {
		return a.Acknowledger.Nack(ctx, cause)
	}

	return nil
}
