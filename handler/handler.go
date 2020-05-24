package handler

import (
	"context"

	"github.com/dogmatiq/infix/parcel"
)

// Handler is an interface for handling messages.
type Handler interface {
	// HandleMessage handles the message in p.
	HandleMessage(ctx context.Context, w UnitOfWork, p parcel.Parcel) error
}
