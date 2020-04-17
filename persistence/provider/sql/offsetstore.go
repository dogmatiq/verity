package sql

import (
	"context"
	"database/sql"

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
// stores the event stream offset associated with a specific application in an
// SQL database.
type offsetStoreRepository struct {
	db *sql.DB
	d  Driver
}

// LoadOffset loads the offset associated with a specific application.
func (r *offsetStoreRepository) LoadOffset(
	ctx context.Context,
	ak string,
) (offsetstore.Offset, error) {
	panic("not implemented")
}
