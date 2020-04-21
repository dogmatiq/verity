package aggregate

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/marshalkit"
)

// Loader loads aggregate instances from persistence.
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
	root dogma.AggregateRoot,
) (*aggregatestore.MetaData, error) {
	md, err := l.AggregateStore.LoadMetaData(ctx, hk, id)
	if err != nil {
		return nil, err
	}

	if root == dogma.StatelessAggregateRoot {
		return md, nil
	}

	if !md.InstanceExists() {
		return md, nil
	}

	if err := l.queryEvents(ctx, md, root); err != nil {
		return nil, err
	}

	return md, nil
}

func (l *Loader) queryEvents(
	ctx context.Context,
	md *aggregatestore.MetaData,
	root dogma.AggregateRoot,
) error {
	// TODO: https://github.com/dogmatiq/dogma/issues/113
	// How do we best configure the filter to deal with events that the
	// aggregate once produced, but no longer does?
	q := eventstore.Query{
		MinOffset:           md.BeginOffset,
		AggregateHandlerKey: md.HandlerKey,
		AggregateInstanceID: md.InstanceID,
	}

	res, err := l.EventStore.QueryEvents(ctx, q)
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

		root.ApplyEvent(p.Message)
	}
}
