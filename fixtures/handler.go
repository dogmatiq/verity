package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/parcel"
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
