package memorypersistence

import (
	"context"

	"github.com/dogmatiq/verity/persistence"
)

// LoadOffset loads the offset associated with a specific application.
func (ds *dataStore) LoadOffset(
	_ context.Context,
	ak string,
) (uint64, error) {
	ds.db.mutex.RLock()
	defer ds.db.mutex.RUnlock()

	return ds.db.offset.offsets[ak], nil
}

// VisitSaveOffset returns an error if a "SaveOffset" operation can not be
// applied to the database.
func (v *validator) VisitSaveOffset(
	_ context.Context,
	op persistence.SaveOffset,
) error {
	if op.CurrentOffset == v.db.offset.offsets[op.ApplicationKey] {
		return nil
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitSaveOffset applies the changes in a "SaveOffset" operation to the
// database.
func (c *committer) VisitSaveOffset(
	_ context.Context,
	op persistence.SaveOffset,
) error {
	c.db.offset.save(op.ApplicationKey, op.NextOffset)
	return nil
}

// offsetDatabase contains event stream offset related data.
type offsetDatabase struct {
	offsets map[string]uint64
}

// save associates an offset with an application key.
func (db *offsetDatabase) save(ak string, offset uint64) {
	if db.offsets == nil {
		db.offsets = map[string]uint64{}
	}

	db.offsets[ak] = offset
}
