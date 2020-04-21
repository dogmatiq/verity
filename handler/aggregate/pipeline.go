package aggregate

import (
	"context"
	"fmt"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/handler/cache"
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

	// Cache is an in-memory cache of aggregate meta-data and roots.
	Cache *cache.Cache

	// Packer is used to create new parcels for events recorded by the
	// handler.
	Packer *parcel.Packer

	// Logger is the target for log messages produced within the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger
}

// instance is an in-memory representation of the aggregate instance, as stored
// in the cache.
type instance struct {
	metadata *aggregatestore.MetaData
	root     dogma.AggregateRoot
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

	// TODO: https://github.com/dogmatiq/infix/issues/191
	// There is currently no timeout on this context because the handler does
	// not provide timeout hints. Aggregates should probably just use the
	// default message timeout at all times.
	rec, err := s.Cache.Acquire(ctx, id)
	if err != nil {
		return err
	}
	defer rec.Release()

	inst, err := s.load(ctx, id, rec)
	if err != nil {
		return err
	}

	sc := &scope{
		cause:   p,
		packer:  s.Packer,
		handler: s.Identity,
		logger:  s.Logger,
		id:      id,
		root:    inst.root,
		exists:  inst.metadata.InstanceExists(),
	}

	s.Handler.HandleCommand(sc, p.Message)

	sc.validate()

	if err := s.save(
		ctx,
		req,
		res,
		inst,
		sc,
	); err != nil {
		return err
	}

	rec.MarkInstanceSaved()

	return nil
}

// load loads an aggregate instance, either from the given cache record, or
// using the loader if the cache record is empty.
func (s *Sink) load(
	ctx context.Context,
	id string,
	rec *cache.Record,
) (*instance, error) {
	if rec.Instance != nil {
		return rec.Instance.(*instance), nil
	}

	root := s.new()
	md, err := s.Loader.Load(
		ctx,
		s.Identity.Key,
		id,
		root,
	)
	if err != nil {
		return nil, err
	}

	inst := &instance{md, root}
	rec.Instance = inst

	return inst, nil
}

// save persists newly recorded events and updates the instance's meta-data.
func (s *Sink) save(
	ctx context.Context,
	req pipeline.Request,
	res *pipeline.Response,
	inst *instance,
	sc *scope,
) error {
	if len(sc.events) == 0 {
		return nil
	}

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

	inst.metadata.SetLastRecordedOffset(offset)

	if !sc.exists {
		inst.root = s.new()
		inst.metadata.MarkInstanceDestroyed()
	}

	if err := tx.SaveAggregateMetaData(ctx, inst.metadata); err != nil {
		return err
	}

	// update the revision so that it remains current in the cache
	inst.metadata.Revision++

	return nil
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
