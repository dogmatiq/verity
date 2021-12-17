package persistence

import (
	"context"
	"github.com/dogmatiq/marshalkit"
)

// AggregateMetaData contains meta-data about an aggregate instance.
type AggregateMetaData struct {
	// HandlerKey is the identity key of the aggregate message handler.
	HandlerKey string

	// InstanceID is the aggregate instance ID.
	InstanceID string

	// Revision is the instance's current version, used to enforce optimistic
	// concurrency control.
	Revision uint64

	// InstanceExists is true if the instance currently exists.
	//
	// When an aggregate instance is destroyed, its meta-data is retained but
	// this flag is set to false.
	InstanceExists bool

	// LastEventID is the ID of the most recent event message recorded against
	// the instance.
	LastEventID string

	// BarrierEventID is the ID of the event message to use as the "barrier
	// message" when loading the instance's historical events.
	//
	// It is updated when the instance is destroyed to avoid loading any events
	// prior to that point.
	BarrierEventID string
}

type AggregateSnapshot struct {
	// HandlerKey is the identity key of the aggregate message handler.
	HandlerKey string

	// InstanceID is the aggregate instance ID.
	InstanceID string

	// LastEventID is the most recent event ID applied to the aggregate root at the point this snapshot was taken.
	LastEventID string

	// Packet contains the binary representation of the aggregate snapshot.
	Packet marshalkit.Packet
}

// AggregateRepository is an interface for reading aggregate state.
type AggregateRepository interface {
	// LoadAggregateMetaData loads the meta-data for an aggregate instance.
	//
	// hk is the aggregate handler's identity key, id is the instance ID.
	LoadAggregateMetaData(
		ctx context.Context,
		hk, id string,
	) (AggregateMetaData, error)

	// LoadAggregateSnapshot loads the latest snapshot for an aggregate instance.
	//
	// hk is the aggregate handler's identity key, id is the instance ID.
	LoadAggregateSnapshot(
		ctx context.Context,
		hk, id string,
	) (AggregateSnapshot, bool, error)
}

// SaveAggregateMetaData is an Operation that creates or updates meta-data about
// an aggregate instance.
type SaveAggregateMetaData struct {
	// MetaData is the meta-data to persist.
	//
	// MetaData.Revision must be the revision of the aggregate instance as
	// currently persisted, otherwise an optimistic concurrency conflict occurs
	// and the entire batch of operations is rejected.
	MetaData AggregateMetaData
}

// AcceptVisitor calls v.VisitSaveAggregateMetaData().
func (op SaveAggregateMetaData) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitSaveAggregateMetaData(ctx, op)
}

func (op SaveAggregateMetaData) entityKey() entityKey {
	return entityKey{"handler", op.MetaData.HandlerKey, op.MetaData.InstanceID}
}

// SaveAggregateSnapshot is an Operation that creates or updates snapshots of
// an aggregate instance.
type SaveAggregateSnapshot struct {
	// Snapshot is the snapshot to persist.
	Snapshot AggregateSnapshot
}

// AcceptVisitor calls v.VisitSaveAggregateSnapshot().
func (op SaveAggregateSnapshot) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitSaveAggregateSnapshot(ctx, op)
}

func (op SaveAggregateSnapshot) entityKey() entityKey {
	return entityKey{"snapshot", op.Snapshot.HandlerKey, op.Snapshot.InstanceID}
}

// RemoveAggregateSnapshot is an Operation that removes a snapshot of an
// aggregate instance.
//
// The instance's pending timeout messages are removed from the message queue.
type RemoveAggregateSnapshot struct {
	// Instance is the instance to remove.
	Snapshot AggregateSnapshot
}

// AcceptVisitor calls v.VisitRemoveAggregateSnapshot().
func (op RemoveAggregateSnapshot) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitRemoveAggregateSnapshot(ctx, op)
}

func (op RemoveAggregateSnapshot) entityKey() entityKey {
	return entityKey{"handler", op.Snapshot.HandlerKey, op.Snapshot.InstanceID}
}
