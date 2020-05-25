package aggregate

import (
	"context"
	"fmt"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/handler/cache"
	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
)

// Adaptor exposes a dogma.AggregateMessageHandler as a handler.Handler.
type Adaptor struct {
	// Identity is the handler's identity.
	Identity *envelopespec.Identity

	// Handler is the aggregate message handler that implements the
	// application-specific message handling logic.
	Handler dogma.AggregateMessageHandler

	// Loader is used to load aggregate instances into memory.
	Loader *Loader

	// Cache is an in-memory cache of aggregate instances.
	Cache cache.Cache

	// Packer is used to create new parcels for events recorded by the
	// handler.
	Packer *parcel.Packer

	// LoadTimeout is the timeout duration allowed while loading aggregate
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
		configkit.AggregateHandlerType,
		&err,
		"",
	)

	id := a.route(p)

	rec, err := a.load(ctx, w, id)
	if err != nil {
		return err
	}

	sc := &scope{
		work:     w,
		cause:    p,
		identity: a.Identity,
		handler:  a.Handler,
		packer:   a.Packer,
		logger:   a.Logger,
		instance: rec.Instance.(*Instance),
	}

	a.Handler.HandleCommand(sc, p.Message)
	sc.validate()

	if sc.lastEventID == "" {
		// No events were recorded at all, so there's no reason to update the
		// meta-data.
		return nil
	}

	w.Do(persistence.SaveAggregateMetaData{
		MetaData: sc.instance.MetaData,
	})

	if sc.instance.MetaData.InstanceExists {
		sc.instance.MetaData.Revision++
	} else {
		a.Cache.Discard(rec)
	}

	return nil
}

// route returns the instance ID that the message in p is routed to, or panics
// if the handler returns an empty string.
func (a *Adaptor) route(p parcel.Parcel) string {
	if id := a.Handler.RouteCommandToInstance(p.Message); id != "" {
		return id
	}

	panic(fmt.Sprintf(
		"%T.RouteCommandToInstance() returned an empty instance ID while routing a %T command",
		a.Handler,
		p.Message,
	))
}

// load obtains an aggregate instance from the cache, falling back to a.Loader
// for uncached instances.
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

// mustNew returns a new aggregate root created by the handler, or panics if the
// handler returns nil.
func mustNew(h dogma.AggregateMessageHandler) dogma.AggregateRoot {
	if r := h.New(); r != nil {
		return r
	}

	panic(fmt.Sprintf("%T.New() returned nil", h))
}
