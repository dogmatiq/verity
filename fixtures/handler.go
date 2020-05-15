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

	HandleMessageFunc func(context.Context, *handler.UnitOfWork, *parcel.Parcel) error
}

// HandleMessage handles the message in p.
func (h *HandlerStub) HandleMessage(ctx context.Context, w *handler.UnitOfWork, p *parcel.Parcel) error {
	if h.HandleMessageFunc != nil {
		return h.HandleMessageFunc(ctx, w, p)
	}

	if h.Handler != nil {
		return h.Handler.HandleMessage(ctx, w, p)
	}

	return nil
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
