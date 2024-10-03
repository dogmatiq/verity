package process

import (
	"context"
	"fmt"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/marshaler"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/verity/handler"
	"github.com/dogmatiq/verity/handler/cache"
	"github.com/dogmatiq/verity/internal/mlog"
	"github.com/dogmatiq/verity/parcel"
	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/queue"
)

// Adaptor exposes a dogma.ProcessMessageHandler as a handler.Handler.
type Adaptor struct {
	// Identity is the handler's identity.
	Identity *envelopespec.Identity

	// Handler is the process message handler that implements the
	// application-specific message handling logic.
	Handler dogma.ProcessMessageHandler

	// Loader is used to load process instances into memory.
	Loader *Loader

	// Marshaler is used to marshal process instances.
	Marshaler marshaler.Marshaler

	// Cache is an in-memory cache of process instances.
	Cache cache.Cache

	// Queue is the message queue that stores pending timeout messages.
	Queue *queue.Queue

	// Packer is used to create new parcels for messages produced by the
	// handler.
	Packer *parcel.Packer

	// LoadTimeout is the timeout duration allowed while loading process
	// state.
	LoadTimeout time.Duration

	// Logger is the target for log messages produced within the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger
}

// HandleMessage handles the message in p.
func (a *Adaptor) HandleMessage(
	ctx context.Context,
	w handler.UnitOfWork,
	p parcel.Parcel,
) (err error) {
	defer mlog.LogHandlerResult(
		a.Logger,
		p.Envelope,
		a.Identity,
		configkit.ProcessHandlerType,
		&err,
		"",
	)

	id, ok, err := a.route(ctx, p)
	if !ok || err != nil {
		return err
	}

	rec, err := a.load(ctx, w, id)
	if err != nil {
		return err
	}

	inst := rec.Instance.(*Instance)
	exists := inst.Revision != 0

	if !exists && !p.ScheduledFor.IsZero() {
		// If the process instance does not exist and the message is a timeout
		// we can just ignore it. Such timeout messages should have been removed
		// from the queue, but they may still make it this far if they were
		// scheduled to occur at around the same time that the instance ended.
		return nil
	}

	sc := &scope{
		work:       w,
		cause:      p,
		identity:   a.Identity,
		handler:    a.Handler,
		packer:     a.Packer,
		logger:     a.Logger,
		instanceID: inst.InstanceID,
	}

	if err := a.handle(ctx, inst.Root, sc, p); err != nil {
		return err
	}

	if !sc.ended {
		for _, p := range sc.timeouts {
			w.ScheduleTimeout(p)
		}

		return a.save(w, inst)
	}

	if exists {
		w.Do(persistence.RemoveProcessInstance{
			Instance: inst.ProcessInstance,
		})

		w.Defer(func(_ handler.Result, err error) {
			if err == nil {
				a.Queue.RemoveTimeoutsByProcessID(a.Identity.GetKey(), id)
			}
		})

		a.Cache.Discard(rec)
	}

	return nil
}

// route returns the instance ID that the message in p is routed to, if any.
//
// If the message is a timeout it is routed to the instance that created it,
// otherwise the Dogma handler's RouteEventToInstance() method is called.
//
// It panics if the handler indicates that an event must be routed to an
// instance but also returns an empty instance ID.
func (a *Adaptor) route(ctx context.Context, p parcel.Parcel) (string, bool, error) {
	if !p.ScheduledFor.IsZero() {
		// The message is a timeout, always route it to the source instance.
		return p.Envelope.SourceInstanceId, true, nil
	}

	// Otherwise, the message is an event, and the handler decides which
	// instance to route to, if any.
	id, ok, err := a.Handler.RouteEventToInstance(ctx, p.Message.(dogma.Event))
	if err != nil {
		return "", false, err
	}

	if ok && id == "" {
		panic(fmt.Sprintf(
			"%T.RouteEventToInstance() returned an empty instance ID while routing a %T event",
			a.Handler,
			p.Message,
		))
	}

	return id, ok, nil
}

// load obtains an process instance from the cache, falling back to a.Loader for
// uncached instances.
func (a *Adaptor) load(
	ctx context.Context,
	w handler.UnitOfWork,
	id string,
) (*cache.Record, error) {
	ctx, cancel := context.WithTimeout(ctx, a.LoadTimeout)
	defer cancel()

	// Lock the cache record for this instance and bind it to the lifetime of w.
	rec, err := a.Cache.Acquire(ctx, w, id)
	if err != nil {
		return nil, err
	}

	if rec.Instance == nil {
		// Otherwise, we need to load the instance from the data-store.
		rec.Instance, err = a.Loader.Load(
			ctx,
			a.Identity.Key,
			id,
			mustNew(a.Handler),
		)
		if err != nil {
			return nil, err
		}
	}

	return rec, nil
}

// save saves the process instance.
func (a *Adaptor) save(
	w handler.UnitOfWork,
	inst *Instance,
) error {
	// An empty packet represents a stateless process root, so we only populate
	// the packet if the root is stafeful.
	if inst.Root != dogma.StatelessProcessRoot {
		var err error
		inst.Packet, err = a.Marshaler.Marshal(inst.Root)
		if err != nil {
			return err
		}
	}

	w.Do(persistence.SaveProcessInstance{
		Instance: inst.ProcessInstance,
	})

	inst.Revision++

	return nil
}

// handle dispatches the message to the Dogma handler.
func (a *Adaptor) handle(
	ctx context.Context,
	r dogma.ProcessRoot,
	sc *scope,
	p parcel.Parcel,
) error {
	if p.ScheduledFor.IsZero() {
		return a.Handler.HandleEvent(ctx, r, sc, p.Message.(dogma.Event))
	}

	return a.Handler.HandleTimeout(ctx, r, sc, p.Message.(dogma.Timeout))
}

// mustNew returns a new process root created by the handler, or panics if the
// handler returns nil.
func mustNew(h dogma.ProcessMessageHandler) dogma.ProcessRoot {
	if r := h.New(); r != nil {
		return r
	}

	panic(fmt.Sprintf("%T.New() returned nil", h))
}
