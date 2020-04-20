package sql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
)

// aggregateStoreDriver is the subset of the Driver interface that is concerned
// with the aggregatestore subsystem.
type aggregateStoreDriver interface {
	// InsertAggregateMetaData inserts meta-data for an aggregate instance.
	//
	// It returns false if the row already exists.
	InsertAggregateMetaData(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		md *aggregatestore.MetaData,
	) (bool, error)

	// UpdateAggregateMetaData updates meta-data for an aggregate instance.
	//
	// It returns false if the row does not exist or md.Revision is not current.
	UpdateAggregateMetaData(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		md *aggregatestore.MetaData,
	) (bool, error)

	// SelectAggregateMetaData selects an aggregate instance's meta-data.
	SelectAggregateMetaData(
		ctx context.Context,
		db *sql.DB,
		ak, hk, id string,
	) (*aggregatestore.MetaData, error)
}

// SaveAggregateMetaData persists meta-data about an aggregate instance.
//
// md.Revision must be the revision of the instance as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the meta-data is
// not saved and ErrConflict is returned.
func (t *transaction) SaveAggregateMetaData(
	ctx context.Context,
	md *aggregatestore.MetaData,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	op := t.ds.driver.InsertAggregateMetaData
	if md.Revision > 0 {
		op = t.ds.driver.UpdateAggregateMetaData
	}

	ok, err := op(
		ctx,
		t.actual,
		t.ds.appKey,
		md,
	)
	if ok || err != nil {
		return err
	}

	return aggregatestore.ErrConflict
}

// aggregateStoreRepository is an implementation of aggregatestore.Repository
// that stores aggregate state in an SQL database.
type aggregateStoreRepository struct {
	db     *sql.DB
	driver Driver
	appKey string
}

// LoadMetaData loads the meta-data for an aggregate instance.
//
// ak is the aggregate handler's identity key, id is the instance ID.
func (r *aggregateStoreRepository) LoadMetaData(
	ctx context.Context,
	hk, id string,
) (*aggregatestore.MetaData, error) {
	return r.driver.SelectAggregateMetaData(ctx, r.db, r.appKey, hk, id)
}
