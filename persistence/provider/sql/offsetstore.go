package sql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/persistence"
)

// offsetStoreDriver is the subset of the Driver interface that is concerned
// with the offsetstore subsystem.
type offsetStoreDriver interface {
	// LoadOffset loads the last offset associated with the given source
	// application key sk. ak is the 'owner' application key.
	//
	// If there is no offset associated with the given source application key,
	// the offset is returned as zero and error as nil.
	LoadOffset(
		ctx context.Context,
		db *sql.DB,
		ak, sk string,
	) (uint64, error)

	// InsertOffset inserts a new offset associated with the given source
	// application key sk. ak is the 'owner' application key.
	//
	// It returns false if the row already exists.
	InsertOffset(
		ctx context.Context,
		tx *sql.Tx,
		ak, sk string,
		n uint64,
	) (bool, error)

	// UpdateOffset updates the offset associated with the given source
	// application key sk. ak is the 'owner' application key.
	//
	// It returns false if the row does not exist or c is not the current offset
	// associated with the given application key.
	UpdateOffset(
		ctx context.Context,
		tx *sql.Tx,
		ak, sk string,
		c, n uint64,
	) (bool, error)
}

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

	if ok, err := t.upsertOffset(
		ctx,
		t.actual,
		ak,
		c, n,
	); ok || err != nil {
		return err
	}

	return persistence.ErrConflict
}

// upsertOffset calls driver's method UpdateOffset() if the source application
// current offset is greater than zero. Otherwise, it call driver's method
// InsertOffset().
func (t *transaction) upsertOffset(
	ctx context.Context,
	tx *sql.Tx,
	sk string,
	c, n uint64,
) (bool, error) {
	if c > 0 {
		return t.ds.driver.UpdateOffset(
			ctx,
			tx,
			t.ds.appKey,
			sk,
			c, n,
		)
	}

	return t.ds.driver.InsertOffset(
		ctx,
		t.actual,
		t.ds.appKey,
		sk,
		n,
	)
}

// offsetStoreRepository is an implementation of offsetstore.Repository that
// stores the event stream offset associated with a specific application in an
// SQL database.
type offsetStoreRepository struct {
	db     *sql.DB
	d      Driver
	appKey string
}

// LoadOffset loads the offset associated with a specific application.
func (r *offsetStoreRepository) LoadOffset(
	ctx context.Context,
	ak string,
) (uint64, error) {
	return r.d.LoadOffset(ctx, r.db, r.appKey, ak)
}
