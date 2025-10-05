package memorypersistence

import (
	"context"

	"github.com/dogmatiq/verity/persistence"
)

// LoadAggregateMetaData loads the meta-data for an aggregate instance.
//
// hk is the aggregate handler's identity key, id is the instance ID.
func (ds *dataStore) LoadAggregateMetaData(
	_ context.Context,
	hk, id string,
) (persistence.AggregateMetaData, error) {
	ds.db.mutex.RLock()
	defer ds.db.mutex.RUnlock()

	key := instanceKey{hk, id}
	if md, ok := ds.db.aggregate.metadata[key]; ok {
		return md, nil
	}

	return persistence.AggregateMetaData{
		HandlerKey: hk,
		InstanceID: id,
	}, nil
}

// VisitSaveAggregateMetaData returns an error if a "SaveAggregateMetaData"
// operation can not be applied to the database.
func (v *validator) VisitSaveAggregateMetaData(
	_ context.Context,
	op persistence.SaveAggregateMetaData,
) error {
	key := instanceKey{op.MetaData.HandlerKey, op.MetaData.InstanceID}
	old := v.db.aggregate.metadata[key]

	if op.MetaData.Revision == old.Revision {
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
	c.db.aggregate.save(op.MetaData)
	return nil
}

// aggregateDatabase contains aggregate related data.
type aggregateDatabase struct {
	metadata map[instanceKey]persistence.AggregateMetaData
}

// save stores md in the database.
func (db *aggregateDatabase) save(md persistence.AggregateMetaData) {
	key := instanceKey{md.HandlerKey, md.InstanceID}

	if db.metadata == nil {
		db.metadata = map[instanceKey]persistence.AggregateMetaData{}
	}

	md.Revision++
	db.metadata[key] = md
}
