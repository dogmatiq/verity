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

	// Cache is an in-memory cache of aggregate meta-data and roots.
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
	w *handler.UnitOfWork,
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

	inst, err := a.load(ctx, w, p)
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
		instance: inst,
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

	// We can safely increment the revision now, knowing that the cache record
	// will be discarded if the unit-of-work is not persisted correctly.
	sc.instance.MetaData.Revision++

	return nil
}

// load obtains an aggregate instance from the cache, falling back to s.Loader
// for uncached instances.
func (a *Adaptor) load(
	ctx context.Context,
	w *handler.UnitOfWork,
	p parcel.Parcel,
) (*Instance, error) {
	id := mustRoute(a.Handler, p.Message)

	ctx, cancel := context.WithTimeout(ctx, a.LoadTimeout)
	defer cancel()

	// Acquire a lock on the cache record for this instance.
	rec, err := a.Cache.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}

	// Add an observer that releases the lock on the cache record only after the
	// unit-of-work is complete.
	w.Observe(func(_ handler.Result, err error) {
		if err == nil {
			// The unit-of-work was persisted successfully, so we call
			// KeepAlive() to ensure rec stays in the cache when it is released.
			//
			// If the unit-of-work failed to persist, we have to discard the
			// cached instance, because it is no longer in sync with what's
			// persisted.
			rec.KeepAlive()
		}

		rec.Release()
	})

	if rec.Instance != nil {
		// The cache record was already populated in with an instance, so we
		// don't need to fallback to using the loader.
		return rec.Instance.(*Instance), nil
	}

	// Otherwise, we need to load the aggregate instance from the data-store.
	inst, err := a.Loader.Load(
		ctx,
		a.Identity.Key,
		id,
		mustNew(a.Handler),
	)
	if err != nil {
		return nil, err
	}

	// Finally, we populate the cache record with the instance.
	rec.Instance = inst

	return inst, nil
}

// mustRoute returns the instance ID that m is routed to, or panics if the
// handler returns an empty string.
func mustRoute(h dogma.AggregateMessageHandler, m dogma.Message) string {
	if id := h.RouteCommandToInstance(m); id != "" {
		return id
	}

	panic(fmt.Sprintf("%T.RouteCommandToInstance() returned an empty instance ID while routing a %T command", h, m))
}

// mustNew returns a new aggregate root created by the handler, or panics if the
// handler returns nil.
func mustNew(h dogma.AggregateMessageHandler) dogma.AggregateRoot {
	if r := h.New(); r != nil {
		return r
	}

	panic(fmt.Sprintf("%T.New() returned nil", h))
}
