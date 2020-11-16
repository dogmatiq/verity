package fixtures

import (
	"context"

	"github.com/dogmatiq/verity/handler"
	"github.com/dogmatiq/verity/parcel"
	"github.com/dogmatiq/verity/persistence"
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
	Commands   []parcel.Parcel
	Events     []parcel.Parcel
	Timeouts   []parcel.Parcel
	Operations []persistence.Operation
	Deferred   []handler.DeferFunc
}

// ExecuteCommand updates the unit-of-work to execute the command in p.
func (w *UnitOfWorkStub) ExecuteCommand(p parcel.Parcel) {
	w.Commands = append(w.Commands, p)
}

// ScheduleTimeout updates the unit-of-work to schedule the timeout in p.
func (w *UnitOfWorkStub) ScheduleTimeout(p parcel.Parcel) {
	w.Timeouts = append(w.Timeouts, p)
}

// RecordEvent updates the unit-of-work to record the event in p.
func (w *UnitOfWorkStub) RecordEvent(p parcel.Parcel) {
	w.Events = append(w.Events, p)
}

// Do updates the unit-of-work to include op in the persistence batch.
func (w *UnitOfWorkStub) Do(op persistence.Operation) {
	w.Operations = append(w.Operations, op)
}

// Defer registers fn to be called when the unit-of-work is complete.
func (w *UnitOfWorkStub) Defer(fn handler.DeferFunc) {
	w.Deferred = append(w.Deferred, fn)
}

// Succeed invokes the unit-of-work's deferred functions with the given result.
func (w *UnitOfWorkStub) Succeed(res handler.Result) {
	w.invokeDeferred(res, nil)
}

// Fail invokes the unit-of-work's deferred functions with the given error.
func (w *UnitOfWorkStub) Fail(err error) {
	w.invokeDeferred(handler.Result{}, err)
}

func (w *UnitOfWorkStub) invokeDeferred(res handler.Result, err error) {
	for i := len(w.Deferred) - 1; i >= 0; i-- {
		w.Deferred[i](res, err)
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
