package memory

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
)

// SaveAggregateMetaData persists meta-data about an aggregate instance.
//
// md.Revision must be the revision of the instance as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the meta-data is
// not saved and ErrConflict is returned.
func (t *transaction) SaveAggregateMetaData(
	ctx context.Context,
	md *aggregatestore.MetaData,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	if t.aggregate.stageSave(&t.ds.db.aggregate, md) {
		return nil
	}

	return persistence.ErrConflict
}

// aggregateStoreRepository is an implementation of aggregatestore.Repository
// that stores aggregate state in memory.
type aggregateStoreRepository struct {
	db *database
}

// LoadMetaData loads the meta-data for an aggregate instance.
//
// ak is the aggregate handler's identity key, id is the instance ID.
func (r *aggregateStoreRepository) LoadMetaData(
	ctx context.Context,
	hk, id string,
) (*aggregatestore.MetaData, error) {
	if err := r.db.RLock(ctx); err != nil {
		return nil, err
	}
	defer r.db.RUnlock()

	if md, ok := r.db.aggregate.metadata[hk][id]; ok {
		return cloneAggregateStoreMetaData(md), nil
	}

	return &aggregatestore.MetaData{
		HandlerKey: hk,
		InstanceID: id,
	}, nil
}

// aggregateStoreChangeSet contains modifications to the aggregate store that have
// been performed within a transaction but not yet committed.
type aggregateStoreChangeSet struct {
	metadata map[string]map[string]*aggregatestore.MetaData
}

// stageSave adds a "SaveAggregateMetaData" operation to the change-set.
//
// It returns false if there is an OCC conflict.
func (cs *aggregateStoreChangeSet) stageSave(
	db *aggregateStoreDatabase,
	md *aggregatestore.MetaData,
) bool {
	// Capture the staged instance ID -> meta-data map once for this handler to
	// minimize the number of lookups in cs.metadata.
	instances := cs.metadata[md.HandlerKey]

	// Get both the committed meta-data, and the meta-data as it appears with
	// its changes staged in this change-set.
	committed := db.metadata[md.HandlerKey][md.InstanceID]
	staged, changed := instances[md.InstanceID]

	// The "effective" meta-data is how the meta-data appears to this
	// transaction.
	effective := committed
	if changed {
		effective = staged
	}

	// Likewise, the "effective" revision is the revision as it appears
	// including the changes in this transaction.
	var effectiveRev uint64
	if effective != nil {
		effectiveRev = effective.Revision
	}

	// Enforce the optimistic concurrency control requirements.
	if md.Revision != effectiveRev {
		return false
	}

	if staged == nil {
		// If we don't have a staged value because this meta-data has not been
		// modified within this change-set, clone the meta-data we're given and
		// add it to the change-set.
		staged = cloneAggregateStoreMetaData(md)

		if instances == nil {
			instances = map[string]*aggregatestore.MetaData{}

			if cs.metadata == nil {
				cs.metadata = map[string]map[string]*aggregatestore.MetaData{}
			}
			cs.metadata[md.HandlerKey] = instances
		}

		instances[md.InstanceID] = staged
	} else {
		// Otherwise, we already have our own clone, just mutate it to match the
		// new meta-data.
		copyAggregateStoreMetaData(staged, md)
	}

	staged.Revision++

	return true
}

// aggregateStoreDatabase contains data that is committed to the aggregate store.
type aggregateStoreDatabase struct {
	metadata map[string]map[string]*aggregatestore.MetaData
}

// apply updates the database to include the changes in cs.
func (db *aggregateStoreDatabase) apply(cs *aggregateStoreChangeSet) {
	for hk, instances := range cs.metadata {
		committed := db.metadata[hk]

		if committed == nil {
			committed = map[string]*aggregatestore.MetaData{}

			if db.metadata == nil {
				db.metadata = map[string]map[string]*aggregatestore.MetaData{}
			}
			db.metadata[hk] = committed
		}

		for id, rev := range instances {
			committed[id] = rev
		}
	}
}

// copyAggregateStoreMetaData assigns meta-data from src to dest.
func copyAggregateStoreMetaData(dest, src *aggregatestore.MetaData) {
	// Yes, this seems like a bit of a useless function. Right now the entire
	// struct is copied, but if anything needs to be deeply-cloned, or *not*
	// copied in the future (as is the case with the queuestore, for example),
	// it can be handled here.
	*dest = *src
}

// cloneAggregateStoreMetaData returns a deep clone of an
// aggregatestore.MetaData.
func cloneAggregateStoreMetaData(md *aggregatestore.MetaData) *aggregatestore.MetaData {
	// Yes, this seems like a bit of a useless function. Right now all of the
	// values in the struct are scalars so it's a non issue, but if there's
	// anything that actually needs deep cloning in the future it can be handled
	// here.
	clone := *md
	return &clone
}
