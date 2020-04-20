package aggregate

import (
	"context"
	"fmt"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/syncx"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
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

	// Handler is the aggregate message handler that implements the
	// application-specific message handling logic.
	Handler dogma.AggregateMessageHandler

	// Loader is used to load aggregate instances into memory.
	Loader *Loader

	// Packer is used to create new parcels for events recorded by the
	// handler.
	Packer *parcel.Packer

	// Logger is the target for log messages produced within the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger

	mutexes syncx.MutexNamespace
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

	id := s.route(p.Message)
	root := s.new()

	unlock, err := s.mutexes.Lock(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()

	md, err := s.Loader.Load(ctx, s.Identity.Key, id, root)
	if err != nil {
		return err
	}

	sc := &scope{
		cause:   p,
		packer:  s.Packer,
		handler: s.Identity,
		logger:  s.Logger,
		id:      id,
		root:    root,
		exists:  md.InstanceExists(),
	}

	s.Handler.HandleCommand(sc, p.Message)

	sc.validate()

	if len(sc.events) > 0 {
		return s.save(ctx, req, res, md, sc)
	}

	return nil
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

	var offset eventstore.Offset
	for _, p := range sc.events {
		offset, err = res.RecordEvent(ctx, tx, p)
		if err != nil {
			return err
		}
	}

	md.SetLastRecordedOffset(offset)

	if !sc.exists {
		md.MarkInstanceDestroyed()
	}

	return tx.SaveAggregateMetaData(ctx, md)
}

// route returns the instance ID that m is routed to, or panics if the handler
// returns an empty string.
func (s *Sink) route(m dogma.Message) string {
	if id := s.Handler.RouteCommandToInstance(m); id != "" {
		return id
	}

	panic(fmt.Sprintf(
		"the '%s' aggregate message handler attempted to route a %T command to an empty instance ID",
		s.Identity.Name,
		m,
	))
}

// new returns a new aggregate instance created by the handler, or panics if the
// handler returns nil.
func (s *Sink) new() dogma.AggregateRoot {
	if r := s.Handler.New(); r != nil {
		return r
	}

	panic(fmt.Sprintf(
		"the '%s' aggregate message handler returned a nil root from New()",
		s.Identity.Name,
	))
}
