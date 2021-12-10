package memorypersistence

import (
	"context"

	"github.com/dogmatiq/verity/persistence"
)

// LoadAggregateMetaData loads the meta-data for an aggregate instance.
//
// hk is the aggregate handler's identity key, id is the instance ID.
func (ds *dataStore) LoadAggregateMetaData(
	ctx context.Context,
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

func (ds *dataStore) LoadAggregateSnapshot(
	_ context.Context,
	hk, id string,
) (persistence.AggregateSnapshot, bool, error) {
	ds.db.mutex.RLock()
	defer ds.db.mutex.RUnlock()

	key := instanceKey{hk, id}
	if inst, ok := ds.db.aggregate.snapshot[key]; ok {
		return inst, true, nil
	}

	return persistence.AggregateSnapshot{
		HandlerKey: hk,
		InstanceID: id,
	}, false, nil
}

// VisitSaveAggregateMetaData returns an error if a "SaveAggregateMetaData"
// operation can not be applied to the database.
func (v *validator) VisitSaveAggregateMetaData(
	_ context.Context,
	op persistence.SaveAggregateMetaData,
) error {
	new := op.MetaData
	key := instanceKey{new.HandlerKey, new.InstanceID}
	old := v.db.aggregate.metadata[key]

	if new.Revision == old.Revision {
		return nil
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitSaveAggregateSnapshot returns an error if a "SaveAggregateSnapshot"
// operation can not be applied to the database.
func (v *validator) VisitSaveAggregateSnapshot(
	_ context.Context,
	op persistence.SaveAggregateSnapshot,
) error {
	ss := op.Snapshot

	if ss.HandlerKey != "" && ss.InstanceID != "" {
		return nil
	}

	return persistence.NotFoundError{
		Cause: op,
	}
}

// VisitRemoveAggregateSnapshot returns an error if a "RemoveAggregateSnapshot"
// operation can not be applied to the database.
func (v *validator) VisitRemoveAggregateSnapshot(
	_ context.Context,
	op persistence.RemoveAggregateSnapshot,
) error {
	inst := op.Snapshot
	key := instanceKey{inst.HandlerKey, inst.InstanceID}

	if x, ok := v.db.aggregate.snapshot[key]; ok {
		if inst.Version == x.Version {
			return nil
		}
	}

	return persistence.NotFoundError{
		Cause: op,
	}
}

// VisitSaveAggregateMetaData applies the changes in a "SaveAggregateMetaData"
// operation to the database.
func (c *committer) VisitSaveAggregateMetaData(
	_ context.Context,
	op persistence.SaveAggregateMetaData,
) error {
	c.db.aggregate.saveMetaData(op.MetaData)
	return nil
}

// aggregateDatabase contains aggregate related data.
type aggregateDatabase struct {
	metadata map[instanceKey]persistence.AggregateMetaData
	snapshot map[instanceKey]persistence.AggregateSnapshot
}

// save stores md in the database.
func (db *aggregateDatabase) saveMetaData(md persistence.AggregateMetaData) {
	key := instanceKey{md.HandlerKey, md.InstanceID}

	if db.metadata == nil {
		db.metadata = map[instanceKey]persistence.AggregateMetaData{}
	}

	md.Revision++
	db.metadata[key] = md
}

// VisitSaveAggregateSnapshot applies the changes in a "VisitSaveAggregateSnapshot"
// operation to the database.
func (c *committer) VisitSaveAggregateSnapshot(
	_ context.Context,
	op persistence.SaveAggregateSnapshot,
) error {
	c.db.aggregate.saveSnapshot(op.Snapshot)
	return nil
}

// save stores md in the database.
func (db *aggregateDatabase) saveSnapshot(ss persistence.AggregateSnapshot) {
	key := instanceKey{ss.HandlerKey, ss.InstanceID}

	if db.snapshot == nil {
		db.snapshot = map[instanceKey]persistence.AggregateSnapshot{}
	}
	db.snapshot[key] = ss
}

// VisitRemoveAggregateSnapshot applies the changes in a "RemoveAggregateSnapshot"
// operation to the database.
func (c *committer) VisitRemoveAggregateSnapshot(
	_ context.Context,
	op persistence.RemoveAggregateSnapshot,
) error {
	inst := op.Snapshot
	key := instanceKey{inst.HandlerKey, inst.InstanceID}

	c.db.process.remove(key)
	return nil
}
