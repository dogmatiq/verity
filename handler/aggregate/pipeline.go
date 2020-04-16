package aggregate

import (
	"context"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/pipeline"
)

// Sink is a pipeline sink that coordinates the handling of messages by a
// dogma.AggregateMessageHandler.
//
// The Accept() method conforms to the pipeline.Sink() signature.
type Sink struct {
	// Identity is the handler's identity.
	Identity *envelopespec.Identity

	// EventStore is the repository used to load an aggregate instance's
	// historical events.
	EventStore eventstore.Repository

	// Handler is the aggregate message handler that implements the
	// application-specific message handling logic.
	Handler dogma.AggregateMessageHandler

	// Packer is used to create new parcels for events recorded by the
	// handler.
	Packer *parcel.Packer

	// Logger is the target for log messages produced within the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger
}

// Accept handles a message using s.Handler.
func (s *Sink) Accept(
	ctx context.Context,
	req pipeline.Request,
	res *pipeline.Response,
) error {
	p, err := req.Parcel()
	if err != nil {
		return err
	}

	id := s.Handler.RouteCommandToInstance(p.Message)

	ds := &scope{
		cause:   p,
		packer:  s.Packer,
		handler: s.Identity,
		id:      id,
		logger:  s.Logger,
	}

	s.Handler.HandleCommand(ds, p.Message)

	tx, err := req.Tx(ctx)
	if err != nil {
		return err
	}

	for _, p := range ds.events {
		if _, err := res.RecordEvent(ctx, tx, p); err != nil {
			return err
		}
	}

	return nil
}
