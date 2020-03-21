package controller

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence"
)

// StreamAdaptor presents a Controller as an eventstream.Handler.
type StreamAdaptor struct {
	// Handler is the identity of the handler that the controller manages.
	Handler configkit.Identity

	// Controller orchestrates the handling of events.
	Controller Controller

	// Repository is used to load stream offsets and begin transactions.
	Repository persistence.OffsetRepository
}

// NextOffset returns the next offset to be consumed from the event stream.
//
// id is the identity of the source application.
func (a *StreamAdaptor) NextOffset(
	ctx context.Context,
	id configkit.Identity,
) (uint64, error) {
	return a.Repository.NextOffset(
		ctx,
		id,
		a.Handler,
	)
}

// HandleEvent handles a message consumed from the event stream.
//
// o must be the offset that would be returned by NextOffset(). On success,
// the next call to NextOffset() will return e.Offset + 1.
func (a *StreamAdaptor) HandleEvent(
	ctx context.Context,
	o uint64,
	ev *eventstream.Event,
) error {
	tx, err := a.Repository.Begin(
		ctx,
		a.Handler,
		o,
		ev,
	)
	if err != nil {
		return err
	}
	defer tx.Close()

	if err := a.Controller.Handle(ctx, tx, ev.Envelope); err != nil {
		return tx.Rollback(ctx, err)
	}

	return tx.Commit(ctx)
}
