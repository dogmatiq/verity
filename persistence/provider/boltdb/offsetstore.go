package boltdb

import (
	"context"

	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
)

// SaveOffset persists the "next" offset to be consumed for a specific
// application.
func (t *transaction) SaveOffset(
	ctx context.Context,
	ak string,
	c, n offsetstore.Offset,
) error {
	panic("not implemented")
}

// offsetStoreRepository is an implementation of offsetstore.Repository that
// stores the event stream offset associated with a specific application in a
// BoltDB database.
type offsetStoreRepository struct {
	db *database
}

// LoadOffset loads the offset associated with a specific application.
func (r *offsetStoreRepository) LoadOffset(
	ctx context.Context,
	ak string,
) (offsetstore.Offset, error) {
	panic("not implemented")
}
