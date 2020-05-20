package aggregate

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/marshalkit"
)

// Instance is an in-memory representation of the aggregate instance, as stored
// in the cache.
type Instance struct {
	MetaData aggregatestore.MetaData
	Root     dogma.AggregateRoot
}

// Loader loads aggregate instances from their historical events.
type Loader struct {
	// AggregateStore is the repository used to load an aggregate instance's
	// revisions and snapshots.
	AggregateStore aggregatestore.Repository

	// EventStore is the repository used to load an aggregate instance's
	// historical events.
	EventStore eventstore.Repository

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
	md, err := l.AggregateStore.LoadAggregateMetaData(ctx, hk, id)
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
	md *aggregatestore.MetaData,
	base dogma.AggregateRoot,
) error {
	res, err := l.EventStore.LoadEventsBySource(
		ctx,
		md.HandlerKey,
		md.InstanceID,
		md.LastDestroyedBy,
	)
	if err != nil {
		return err
	}

	for {
		i, ok, err := res.Next(ctx)
		if !ok || err != nil {
			return err
		}

		p, err := parcel.FromEnvelope(l.Marshaler, i.Envelope)
		if err != nil {
			return err
		}

		base.ApplyEvent(p.Message)
	}
}
