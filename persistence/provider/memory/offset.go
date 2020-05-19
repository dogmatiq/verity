package memory

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
)

// offsetRepository is an implementation of offsetstore.Repository that
// stores the event stream offset associated with a specific application in
// memory.
type offsetRepository struct {
	db *database
}

// LoadOffset loads the offset associated with a specific application.
func (r *offsetRepository) LoadOffset(
	ctx context.Context,
	ak string,
) (uint64, error) {
	r.db.mutex.RLock()
	defer r.db.mutex.RUnlock()

	return r.db.offset.offsets[ak], nil
}

// offsetDatabase contains data that is committed to the offset store.
type offsetDatabase struct {
	offsets map[string]uint64
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
	if c.db.offset.offsets == nil {
		c.db.offset.offsets = map[string]uint64{}
	}

	c.db.offset.offsets[op.ApplicationKey] = op.NextOffset

	return nil
}
