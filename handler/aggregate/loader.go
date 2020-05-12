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

	if !md.InstanceExists {
		return md, nil
	}

	if err := l.loadEvents(ctx, md, root); err != nil {
		return nil, err
	}

	return md, nil
}

func (l *Loader) loadEvents(
	ctx context.Context,
	md *aggregatestore.MetaData,
	root dogma.AggregateRoot,
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

		root.ApplyEvent(p.Message)
	}
}
