package aggregate

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
)

// Instance is an in-memory representation of the aggregate instance, as stored
// in the cache.
type Instance struct {
	MetaData persistence.AggregateMetaData
	Root     dogma.AggregateRoot
}

// Loader loads aggregate instances from their historical events.
type Loader struct {
	// AggregateRepository is the repository used to load an aggregate instance's
	// revisions and snapshots.
	AggregateRepository persistence.AggregateRepository

	// EventRepository is the repository used to load an aggregate instance's
	// historical events.
	EventRepository persistence.EventRepository

	// Marshaler is used to marshal/unmarshal aggregate snapshots and historical
	// events,
	Marshaler marshalkit.ValueMarshaler
}

// Load loads the aggregate instance with the given ID.
func (l *Loader) Load(
	ctx context.Context,
	hk, id string,
	base dogma.AggregateRoot,
) (*Instance, error) {
	md, err := l.AggregateRepository.LoadAggregateMetaData(ctx, hk, id)
	if err != nil {
		return nil, err
	}

	inst := &Instance{*md, base}

	if base == dogma.StatelessAggregateRoot {
		return inst, nil
	}

	if !md.InstanceExists {
		return inst, nil
	}

	if err := l.applyEvents(ctx, md, base); err != nil {
		return nil, err
	}

	return inst, nil
}

func (l *Loader) applyEvents(
	ctx context.Context,
	md *persistence.AggregateMetaData,
	base dogma.AggregateRoot,
) error {
	res, err := l.EventRepository.LoadEventsBySource(
		ctx,
		md.HandlerKey,
		md.InstanceID,
		md.LastDestroyedBy,
	)
	if err != nil {
		return err
	}

	for {
		ev, ok, err := res.Next(ctx)
		if !ok || err != nil {
			return err
		}

		p, err := parcel.FromEnvelope(l.Marshaler, ev.Envelope)
		if err != nil {
			return err
		}

		base.ApplyEvent(p.Message)
	}
}
