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

// LoadOffset loads the offset associated with a specific application.
func (ds *dataStore) LoadOffset(
	ctx context.Context,
	ak string,
) (uint64, error) {
	return ds.driver.LoadOffset(ctx, ds.db, ds.appKey, ak)
}

// VisitSaveOffset applies the changes in a "SaveOffset" operation to the
// database.
func (c *committer) VisitSaveOffset(
	ctx context.Context,
	op persistence.SaveOffset,
) error {
	var (
		ok  bool
		err error
	)

	if op.CurrentOffset == 0 {
		ok, err = c.driver.InsertOffset(
			ctx,
			c.tx,
			c.appKey,
			op.ApplicationKey,
			op.NextOffset,
		)
	} else {
		ok, err = c.driver.UpdateOffset(
			ctx,
			c.tx,
			c.appKey,
			op.ApplicationKey,
			op.CurrentOffset,
			op.NextOffset,
		)
	}

	if ok || err != nil {
		return err
	}

	return persistence.ConflictError{
		Cause: op,
	}
}
