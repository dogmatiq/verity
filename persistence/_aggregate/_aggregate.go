package persistence

import (
	"context"

	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/marshalkit"
)

// TODO
// TODO
// TODO  AGGREGATES NEED A REVISION, THEY CAN NOT USE OFFSETS FOR OCC
// TODO
// TODO

// AggregateSnapshotRepository is an interface for reading and writing aggregate
// snapshots.
type AggregateSnapshotRepository interface {
	// LoadAggregateSnapshot loads the most recent snapshot of an aggregate
	// root.
	//
	// hk is the identity key of the aggregate message handler. id is the
	// aggregate instance ID.
	//
	// p is a packet containing the snapshot of the aggregate root. n is the
	// offset of the next event that needs to be applied to the aggregate root.
	//
	// If no snapshot is available, n is 0 and p is invalid.
	LoadAggregateSnapshot(
		ctx context.Context,
		hk, id string,
	) (p marshalkit.Packet, n eventstream.Offset, err error)

	// SaveAggregateSnapshot stores a snapshot of an aggregate root.
	//
	// hk is the identity key of the aggregate message handler. id is the
	// aggregate instance ID.
	//
	// p is a packet containing the snapshot of the aggregate root. o is the
	// offset of the last event produced by this aggregate instance.
	//
	// The implementation may discard this snapshot if newer snapshots are
	// already persisted. Conversely, any older snapshots may be removed by this
	// operation.
	SaveAggregateSnapshot(
		ctx context.Context,
		hk, id string,
		p marshalkit.Packet,
		o eventstream.Offset,
	) error
}

// AggregateTransaction is the subset of the Transaction interface concerned
// with aggregate data.
type AggregateTransaction interface {
	// SaveAggregateInstance updates (or creates) an aggregate instance.
	//
	// hk is the identity key of the aggregate message handler. id is the
	// aggregate instance ID.
	//
	// c must be the offset of the last event produced by the aggregate instance
	// as currently persisted, otherwise an optimistic concurrency conflict has
	// occurred, the aggregate is not saved and ok is false.
	//
	// o is the offset of the last event produced by the aggregate instance
	// within this transaction.
	SaveAggregateInstance(
		ctx context.Context,
		hk, id string,
		c, o eventstream.Offset,
	) (ok bool, err error)

	// DeleteAggregateInstance deletes an aggregate instance and all of its
	// snapshots.
	//
	// hk is the identity key of the aggregate message handler. id is the
	// aggregate instance ID.
	//
	// c must be the offset of the last event produced by the aggregate instance
	// as currently persisted, otherwise an optimistic concurrency conflict has
	// occurred, the aggregate is not deleted and ok is false.
	DeleteAggregateInstance(
		ctx context.Context,
		hk, id string,
		c eventstream.Offset,
	) (ok bool, err error)
}
