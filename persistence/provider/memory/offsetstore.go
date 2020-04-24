package memory

import (
	"context"

	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
)

// SaveOffset persists the "next" offset to be consumed for a specific
// application.
func (t *transaction) SaveOffset(
	ctx context.Context,
	ak string,
	c, n eventstream.Offset,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	if t.offset.stageSave(&t.ds.db.offset, ak, c, n) {
		return nil
	}

	return offsetstore.ErrConflict
}

// offsetStoreChangeSet contains modifications to the offset store that have
// been performed within a transaction but not yet committed.
type offsetStoreChangeSet struct {
	items map[string]eventstream.Offset
}

// stageSave adds a "SaveOffset" operation to the change-set.
//
// It returns false if there is an OCC conflict.
func (cs *offsetStoreChangeSet) stageSave(
	db *offsetStoreDatabase,
	ak string,
	c, n eventstream.Offset,
) bool {
	// Get both the committed item, and the item as it appears with its changes
	// staged in this change-set.
	committed := db.items[ak]
	staged, changed := cs.items[ak]

	// The "effective" item is how the item appears to this transaction.
	effective := committed
	if changed {
		effective = staged
	}

	// Enforce the optimistic concurrency control requirements.
	if effective != c {
		return false
	}

	if cs.items == nil {
		cs.items = map[string]eventstream.Offset{}
	}

	cs.items[ak] = n

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
) (eventstream.Offset, error) {
	if err := r.db.RLock(ctx); err != nil {
		return 0, err
	}
	defer r.db.RUnlock()

	return r.db.offset.items[ak], nil
}

// offsetStoreDatabase contains data that is committed to the offset store.
type offsetStoreDatabase struct {
	items map[string]eventstream.Offset
}

// apply updates the database to include the changes in cs.
func (db *offsetStoreDatabase) apply(cs *offsetStoreChangeSet) {
	if db.items == nil {
		db.items = map[string]eventstream.Offset{}
	}

	for k, v := range cs.items {
		db.items[k] = v
	}
}
