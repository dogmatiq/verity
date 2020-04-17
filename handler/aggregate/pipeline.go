package aggregate

import (
	"context"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/pipeline"
	"github.com/dogmatiq/marshalkit"
)

// Sink is a pipeline sink that coordinates the handling of messages by a
// dogma.AggregateMessageHandler.
//
// The Accept() method conforms to the pipeline.Sink() signature.
type Sink struct {
	// Identity is the handler's identity.
	Identity *envelopespec.Identity

	// Handler is the aggregate message handler that implements the
	// application-specific message handling logic.
	Handler dogma.AggregateMessageHandler

	// AggregateStore is the repository used to load an aggregate instance's
	// revisions and snapshots.
	AggregateStore aggregatestore.Repository

	// EventStore is the repository used to load an aggregate instance's
	// historical events.
	EventStore eventstore.Repository

	// Marshaler is used to marshal/unmarshal aggregate snapshots and historical
	// events,
	Marshaler marshalkit.ValueMarshaler

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

	rev, err := s.AggregateStore.LoadRevision(ctx, s.Identity.Key, id)
	if err != nil {
		return err
	}

	root := s.Handler.New()

	if rev > 0 {
		if err := s.load(ctx, root, id); err != nil {
			return err
		}
	}

	ds := &scope{
		cause:   p,
		packer:  s.Packer,
		handler: s.Identity,
		id:      id,
		exists:  rev > 0,
		root:    root,
		logger:  s.Logger,
	}

	s.Handler.HandleCommand(ds, p.Message)

	if len(ds.events) == 0 {
		return nil
	}

	tx, err := req.Tx(ctx)
	if err != nil {
		return err
	}

	for _, p := range ds.events {
		if _, err := res.RecordEvent(ctx, tx, p); err != nil {
			return err
		}
	}

	return tx.IncrementAggregateRevision(ctx, s.Identity.Key, id, rev)
}

// load applies an aggregate instance's historical events to the root in order
// to reproduce the current state.
func (s *Sink) load(
	ctx context.Context,
	root dogma.AggregateRoot,
	id string,
) error {
	q := eventstore.Query{
		// TODO: how do we best configure the filter to deal with events that
		// the aggregate once produced, but no longer does?
		Filter:              nil,
		AggregateHandlerKey: s.Identity.Key,
		AggregateInstanceID: id,
	}

	res, err := s.EventStore.QueryEvents(ctx, q)
	if err != nil {
		return err
	}

	for {
		i, ok, err := res.Next(ctx)
		if !ok || err != nil {
			return err
		}

		p, err := parcel.FromEnvelope(s.Marshaler, i.Envelope)
		if err != nil {
			return err
		}

		root.ApplyEvent(p.Message)
	}
}
