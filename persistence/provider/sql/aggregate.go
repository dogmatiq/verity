package sql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/persistence"
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
