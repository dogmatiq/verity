package memory

import (
	"context"

	"github.com/dogmatiq/infix/internal/refactor251"
)

// SaveOffset persists the "next" offset to be consumed for a specific
// application.
func (t *transaction) SaveOffset(
	ctx context.Context,
	ak string,
	c, n uint64,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	if t.offset.stageSave(&t.ds.db.offset, ak, c, n) {
		return nil
	}

	return refactor251.ErrConflict
}

// offsetStoreChangeSet contains modifications to the offset store that have
// been performed within a transaction but not yet committed.
type offsetStoreChangeSet struct {
	offsets map[string]uint64
}

// stageSave adds a "SaveOffset" operation to the change-set.
//
// It returns false if there is an OCC conflict.
func (cs *offsetStoreChangeSet) stageSave(
	db *offsetStoreDatabase,
	ak string,
	c, n uint64,
) bool {
	// Get both the committed offset, and the staged offset from this change-set.
	committed := db.offsets[ak]
	staged, changed := cs.offsets[ak]

	// The "effective" value is how the offset appears to this transaction.
	effective := committed
	if changed {
		effective = staged
	}

	// Enforce the optimistic concurrency control requirements.
	if effective != c {
		return false
	}

	if cs.offsets == nil {
		cs.offsets = map[string]uint64{}
	}

	cs.offsets[ak] = n

	return true
}

// offsetStoreRepository is an implementation of offsetstore.Repository that
// stores the event stream offset associated with a specific application in
// memory.
type offsetStoreRepository struct {
	db *database
}

// LoadOffset loads the offset associated with a specific application.
func (r *offsetStoreRepository) LoadOffset(
	ctx context.Context,
	ak string,
) (uint64, error) {
	if err := r.db.RLock(ctx); err != nil {
		return 0, err
	}
	defer r.db.RUnlock()

	return r.db.offset.offsets[ak], nil
}

// offsetStoreDatabase contains data that is committed to the offset store.
type offsetStoreDatabase struct {
	offsets map[string]uint64
}

// apply updates the database to include the changes in cs.
func (db *offsetStoreDatabase) apply(cs *offsetStoreChangeSet) {
	if db.offsets == nil {
		db.offsets = map[string]uint64{}
	}

	for k, v := range cs.offsets {
		db.offsets[k] = v
	}
}
