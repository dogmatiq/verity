package persistence

import "context"

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

	// LastDestroyedBy is the ID of the last event message recorded in when the
	// instance was most recently destroyed.
	LastDestroyedBy string
}

// AggregateRepository is an interface for reading aggregate state.
type AggregateRepository interface {
	// LoadAggregateMetaData loads the meta-data for an aggregate instance.
	//
	// ak is the aggregate handler's identity key, id is the instance ID.
	LoadAggregateMetaData(
		ctx context.Context,
		hk, id string,
	) (*AggregateMetaData, error)
}

// SaveAggregateMetaData is a persistence operation that creates or updates
// meta-data about an aggregate instance.
type SaveAggregateMetaData struct {
	// MetaData is the meta-data to persist.
	//
	// MetaData.Revision must be the revision of the aggregate instance as
	// currently persisted, otherwise an optimistic concurrency conflict occurs
	// and the entire batch of operations is rejected.
	MetaData AggregateMetaData
}
