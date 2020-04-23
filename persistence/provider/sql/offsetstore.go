package sql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
)

// offsetStoreDriver is the subset of the Driver interface that is concerned
// with the offsetstore subsystem.
type offsetStoreDriver interface {
	// LoadOffset loads the last offset associated with the given application
	// key.
	//
	// If there is no offset associated with the given application key, the
	// offset is returned as zero and error as nil.
	LoadOffset(
		ctx context.Context,
		db *sql.DB,
		ak string,
	) (eventstream.Offset, error)

	// InsertOffset inserts a new offset associated with the given application
	// key.
	//
	// It returns false if the row already exists.
	InsertOffset(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		c, n eventstream.Offset,
	) (bool, error)

	// UpdateOffset updates the offset associated with the given application
	// key.
	//
	// It returns false if the row does not exist or c is not the current
	// offset associated with the given application key.
	UpdateOffset(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		c, n eventstream.Offset,
	) (bool, error)
}

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

	op := t.ds.driver.InsertOffset
	if c > 0 {
		op = t.ds.driver.UpdateOffset
	}

	ok, err := op(
		ctx,
		t.actual,
		ak,
		c, n,
	)
	if ok || err != nil {
		return err
	}

	return offsetstore.ErrConflict
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
) (eventstream.Offset, error) {
	o, err := r.d.LoadOffset(ctx, r.db, ak)
	if err == sql.ErrNoRows {
		return 0, nil
	}

	return o, err
}
