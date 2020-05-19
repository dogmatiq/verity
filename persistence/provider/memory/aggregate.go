package memory

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
)

// aggregateRepository is an implementation of aggregatestore.Repository
// that stores aggregate state in memory.
type aggregateRepository struct {
	db *database
}

// LoadMetaData loads the meta-data for an aggregate instance.
//
// ak is the aggregate handler's identity key, id is the instance ID.
func (r *aggregateRepository) LoadMetaData(
	ctx context.Context,
	hk, id string,
) (*aggregatestore.MetaData, error) {
	r.db.mutex.RLock()
	defer r.db.mutex.RUnlock()

	if md, ok := r.db.aggregate.metadata[hk][id]; ok {
		return &md, nil
	}

	return &aggregatestore.MetaData{
		HandlerKey: hk,
		InstanceID: id,
	}, nil
}

// aggregateDatabase contains aggregate related data.
type aggregateDatabase struct {
	metadata map[string]map[string]aggregatestore.MetaData
}

// VisitSaveAggregateMetaData returns an error if a "SaveAggregateMetaData"
// operation can not be applied to the database.
func (v *validator) VisitSaveAggregateMetaData(
	_ context.Context,
	op persistence.SaveAggregateMetaData,
) error {
	new := op.MetaData
	old := v.db.aggregate.metadata[new.HandlerKey][new.InstanceID]

	if new.Revision == old.Revision {
		return nil
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitSaveAggregateMetaData applies the changes in a "SaveAggregateMetaData"
// operation to the database.
func (c *committer) VisitSaveAggregateMetaData(
	_ context.Context,
	op persistence.SaveAggregateMetaData,
) error {
	md := op.MetaData
	instances := c.db.aggregate.metadata[md.HandlerKey]

	if instances == nil {
		instances = map[string]aggregatestore.MetaData{}

		if c.db.aggregate.metadata == nil {
			c.db.aggregate.metadata = map[string]map[string]aggregatestore.MetaData{}
		}

		c.db.aggregate.metadata[md.HandlerKey] = instances
	}

	md.Revision++
	instances[md.InstanceID] = md

	return nil
}
