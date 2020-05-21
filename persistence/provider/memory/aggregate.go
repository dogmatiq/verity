package memory

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
)

// LoadAggregateMetaData loads the meta-data for an aggregate instance.
//
// ak is the aggregate handler's identity key, id is the instance ID.
func (ds *dataStore) LoadAggregateMetaData(
	ctx context.Context,
	hk, id string,
) (persistence.AggregateMetaData, error) {
	ds.db.mutex.RLock()
	defer ds.db.mutex.RUnlock()

	if md, ok := ds.db.aggregate.metadata[hk][id]; ok {
		return md, nil
	}

	return persistence.AggregateMetaData{
		HandlerKey: hk,
		InstanceID: id,
	}, nil
}

// aggregateDatabase contains aggregate related data.
type aggregateDatabase struct {
	metadata map[string]map[string]persistence.AggregateMetaData
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
		instances = map[string]persistence.AggregateMetaData{}

		if c.db.aggregate.metadata == nil {
			c.db.aggregate.metadata = map[string]map[string]persistence.AggregateMetaData{}
		}

		c.db.aggregate.metadata[md.HandlerKey] = instances
	}

	md.Revision++
	instances[md.InstanceID] = md

	return nil
}
