package memory

import (
	"context"

	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
)

// IncrementAggregateRevision increments the persisted revision of a an
// aggregate instance.
//
// ak is the aggregate handler's identity key, id is the instance ID.
//
// c must be the instance's current revision as persisted, otherwise an
// optimistic concurrency conflict has occurred, the revision is not saved and
// ErrConflict is returned.
func (t *transaction) IncrementAggregateRevision(
	ctx context.Context,
	hk string,
	id string,
	c aggregatestore.Revision,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	if t.aggregate.stageIncrement(&t.ds.db.aggregate, hk, id, c) {
		return nil
	}

	return aggregatestore.ErrConflict
}

// aggregateStoreRepository is an implementation of aggregatestore.Repository
// that stores aggregate state in memory.
type aggregateStoreRepository struct {
	db *database
}

// LoadRevision loads the current revision of an aggregate instance.
//
// ak is the aggregate handler's identity key, id is the instance ID.
func (r *aggregateStoreRepository) LoadRevision(
	ctx context.Context,
	hk, id string,
) (aggregatestore.Revision, error) {
	if err := r.db.RLock(ctx); err != nil {
		return 0, err
	}
	defer r.db.RUnlock()

	return r.db.aggregate.revisions[hk][id], nil
}

// aggregateStoreChangeSet contains modifications to the aggregate store that have
// been performed within a transaction but not yet committed.
type aggregateStoreChangeSet struct {
	revisions map[string]map[string]aggregatestore.Revision
}

// stageIncrement adds a "IncrementAggregateRevision" operation to the
// change-set.
//
// It returns false if there is an OCC conflict.
func (cs *aggregateStoreChangeSet) stageIncrement(
	db *aggregateStoreDatabase,
	hk string,
	id string,
	c aggregatestore.Revision,
) bool {
	// Capture the instance ID -> revision map once for this handler to avoid
	// minimize the number of lookups in cs.revisions.
	instances := cs.revisions[hk]

	// Get both the committed item, and the item as it appears with its changes
	// staged in this change-set.
	committed := db.revisions[hk][id]
	staged, changed := instances[id]

	// The "effective" revision is how the revision appears to this transaction.
	effective := committed
	if changed {
		effective = staged
	}

	// Enforce the optimistic concurrency control requirements.
	if c != effective {
		return false
	}

	// Add the incremented revision to the change-set.
	if instances == nil {
		instances = map[string]aggregatestore.Revision{}

		if cs.revisions == nil {
			cs.revisions = map[string]map[string]aggregatestore.Revision{}
		}
		cs.revisions[hk] = instances
	}

	instances[id] = effective + 1

	return true
}

// aggregateStoreDatabase contains data that is committed to the aggregate store.
type aggregateStoreDatabase struct {
	revisions map[string]map[string]aggregatestore.Revision
}

// apply updates the database to include the changes in cs.
func (db *aggregateStoreDatabase) apply(cs *aggregateStoreChangeSet) {
	for hk, instances := range cs.revisions {
		committed := db.revisions[hk]

		if committed == nil {
			committed = map[string]aggregatestore.Revision{}

			if db.revisions == nil {
				db.revisions = map[string]map[string]aggregatestore.Revision{}
			}
			db.revisions[hk] = committed
		}

		for id, rev := range instances {
			committed[id] = rev
		}
	}
}
