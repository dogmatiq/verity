package persistence

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/eventstream"
)

// An OffsetRepository stores the "progress" that event-consuming message
// handlers mave made through the event streams that they consume from.
type OffsetRepository interface {
	// NextOffset returns the offset of the next event to be consumed for a
	// specific source application and handler.
	//
	// a is the identity of the source application, and h is the identity of the
	// handler.
	NextOffset(ctx context.Context, a, h configkit.Identity) (uint64, error)

	// Begin starts a transaction for a message obtained from an application's
	// event stream.
	//
	// h is the identity of the handler that will handle the message. o must be
	// the "next offset" that is currently persisted in the repository.
	//
	// When transaction is committed successfully, the "next offset" is updated
	// to ev.Offset + 1.
	Begin(
		ctx context.Context,
		h configkit.Identity,
		o uint64,
		ev *eventstream.Event,
	) (Transaction, error)
}
