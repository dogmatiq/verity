package aggregate

import (
	"context"
	"fmt"

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
	if id == "" {
		panic(fmt.Sprintf(
			"the '%s' aggregate message handler attempted to route a %T command to an empty instance ID",
			s.Identity.Name,
			p.Message,
		))
	}

	root := s.Handler.New()
	if root == nil {
		panic(fmt.Sprintf(
			"the '%s' aggregate message handler returned a nil root from New()",
			s.Identity.Name,
		))
	}

	md, err := s.AggregateStore.LoadMetaData(ctx, s.Identity.Key, id)
	if err != nil {
		return err
	}

	// The events available for this instance are given by the open interval
	// [min, max).
	//
	// If the instance never existed, both will be 0, indicating an empty range.
	//
	// If the instance did exist but was destroyed, MinOffset is updated to
	// equal MaxOffset (that is, after the event recorded when the instance was
	// destroyed), again indicating an empty range.
	exists := md.MaxOffset > md.MinOffset

	if exists {
		if err := s.load(ctx, md, root); err != nil {
			return err
		}
	}

	sc := &scope{
		cause:   p,
		packer:  s.Packer,
		handler: s.Identity,
		logger:  s.Logger,

		id:     id,
		root:   root,
		exists: exists,
	}

	s.Handler.HandleCommand(sc, p.Message)

	if len(sc.events) == 0 {
		if sc.created {
			panic(fmt.Sprintf(
				"the '%s' aggregate message handler created the '%s' instance without recording an event while handling a %T command",
				s.Identity.Name,
				id,
				p.Message,
			))
		}

		if sc.destroyed {
			panic(fmt.Sprintf(
				"the '%s' aggregate message handler destroyed the '%s' instance without recording an event while handling a %T command",
				s.Identity.Name,
				id,
				p.Message,
			))
		}

		return nil
	}

	return s.save(ctx, req, res, md, sc)
}

// load applies an aggregate instance's historical events to the root in order
// to reproduce the current state.
func (s *Sink) load(
	ctx context.Context,
	md *aggregatestore.MetaData,
	root dogma.AggregateRoot,
) error {
	q := eventstore.Query{
		MinOffset: md.MinOffset,
		// TODO: https://github.com/dogmatiq/dogma/issues/113
		// How do we best configure the filter to deal with events that the
		// aggregate once produced, but no longer does?
		Filter:              nil,
		AggregateHandlerKey: md.HandlerKey,
		AggregateInstanceID: md.InstanceID,
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

// save persists newly recorded events and updates the instance's meta-data.
func (s *Sink) save(
	ctx context.Context,
	req pipeline.Request,
	res *pipeline.Response,
	md *aggregatestore.MetaData,
	sc *scope,
) error {
	tx, err := req.Tx(ctx)
	if err != nil {
		return err
	}

	for _, p := range sc.events {
		md.MaxOffset, err = res.RecordEvent(ctx, tx, p)
		if err != nil {
			return err
		}
	}

	md.MaxOffset++ // max-offset is exclusive, must be last-offset + 1.

	if !sc.exists {
		md.MinOffset = md.MaxOffset
	}

	return tx.SaveAggregateMetaData(ctx, md)
}
