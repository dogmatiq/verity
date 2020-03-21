package controller

import (
	"context"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
)

// A Controller orchestrates the handling of a message, including persisting the
// engine-managed side effects.
type Controller interface {
	// Handle handles a message within the given transaction.
	Handle(context.Context, persistence.Transaction, *envelope.Envelope) error
}
