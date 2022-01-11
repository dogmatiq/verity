package sqlpersistence

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/verity/persistence"
)

// AggregateDriver is the subset of the Driver interface that is concerned with
// aggregates.
type AggregateDriver interface {
	// InsertAggregateMetaData inserts meta-data for an aggregate instance.
	//
	// It returns false if the row already exists.
	InsertAggregateMetaData(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		md persistence.AggregateMetaData,
	) (bool, error)

	// UpdateAggregateMetaData updates meta-data for an aggregate instance.
	//
	// It returns false if the row does not exist or md.Revision is not current.
	UpdateAggregateMetaData(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		md persistence.AggregateMetaData,
	) (bool, error)

	// SelectAggregateMetaData selects an aggregate instance's meta-data.
	SelectAggregateMetaData(
		ctx context.Context,
		db *sql.DB,
		ak, hk, id string,
	) (persistence.AggregateMetaData, error)

	// InsertAggregateSnapshot inserts a snapshot for an aggregate instance.
	//
	// It returns false if the row already exists.
	InsertAggregateSnapshot(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		md persistence.AggregateSnapshot,
	) (bool, error)

	// UpdateAggregateSnapshot updates snapshots for an aggregate instance.
	//
	// It returns false if the row does not exist.
	UpdateAggregateSnapshot(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		md persistence.AggregateSnapshot,
	) (bool, error)

	// DeleteAggregateSnapshot deletes an aggregate snapshot.
	//
	// It returns false if the row does not exist.
	DeleteAggregateSnapshot(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		inst persistence.AggregateSnapshot,
	) (bool, error)

	// SelectAggregateSnapshot selects an aggregate instance's snapshot.
	SelectAggregateSnapshot(
		ctx context.Context,
		db *sql.DB,
		ak, hk, id string,
	) (persistence.AggregateSnapshot, bool, error)
}

// LoadAggregateMetaData loads the meta-data for an aggregate instance.
//
// hk is the aggregate handler's identity key, id is the instance ID.
func (ds *dataStore) LoadAggregateMetaData(
	ctx context.Context,
	hk, id string,
) (persistence.AggregateMetaData, error) {
	return ds.driver.SelectAggregateMetaData(
		ctx,
		ds.db,
		ds.appKey,
		hk,
		id,
	)
}

// LoadAggregateSnapshot loads the snapshot for an aggregate instance.
//
// hk is the aggregate handler's identity key, id is the instance ID.
func (ds *dataStore) LoadAggregateSnapshot(
	ctx context.Context,
	hk, id string,
) (persistence.AggregateSnapshot, bool, error) {
	return ds.driver.SelectAggregateSnapshot(
		ctx,
		ds.db,
		ds.appKey,
		hk,
		id,
	)
}

// VisitSaveAggregateMetaData applies the changes in a "SaveAggregateMetaData"
// operation to the database.
func (c *committer) VisitSaveAggregateMetaData(
	ctx context.Context,
	op persistence.SaveAggregateMetaData,
) error {
	fn := c.driver.InsertAggregateMetaData
	if op.MetaData.Revision > 0 {
		fn = c.driver.UpdateAggregateMetaData
	}

	ok, err := fn(
		ctx,
		c.tx,
		c.appKey,
		op.MetaData,
	)
	if ok || err != nil {
		return err
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitSaveAggregateSnapshot applies the changes in a "VisitSaveAggregateSnapshot"
// operation to the database.
func (c *committer) VisitSaveAggregateSnapshot(
	ctx context.Context,
	op persistence.SaveAggregateSnapshot,
) error {
	var ok bool
	var err error

	ok, err = c.driver.InsertAggregateSnapshot(
		ctx,
		c.tx,
		c.appKey,
		op.Snapshot,
	)
	if ok || err != nil {
		return err
	}

	_, err = c.driver.UpdateAggregateSnapshot(
		ctx,
		c.tx,
		c.appKey,
		op.Snapshot,
	)

	return err
}

// VisitRemoveAggregateSnapshot applies the changes in a "RemoveAggregateSnapshot"
// operation to the database.
func (c *committer) VisitRemoveAggregateSnapshot(
	ctx context.Context,
	op persistence.RemoveAggregateSnapshot,
) error {
	_, err := c.driver.DeleteAggregateSnapshot(
		ctx,
		c.tx,
		c.appKey,
		op.Snapshot,
	)

	return err
}
