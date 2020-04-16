package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
)

// aggregateStoreDriver is the subset of the Driver interface that is concerned
// with the aggregatestore subsystem.
type aggregateStoreDriver interface {
	// InsertAggregateRevision inserts an aggregate revision for an aggregate
	// instance.
	//
	// It returns false if the row already exists.
	InsertAggregateRevision(
		ctx context.Context,
		tx *sql.Tx,
		ak, hk, id string,
	) (bool, error)

	// UpdateAggregateRevision increments an aggregate isntance's revision by 1.
	//
	// It returns false if the row does not exist or rev is not current.
	UpdateAggregateRevision(
		ctx context.Context,
		tx *sql.Tx,
		ak, hk, id string,
		rev aggregatestore.Revision,
	) (bool, error)

	// SelectAggregateRevision selects an aggregate instance's revision.
	SelectAggregateRevision(
		ctx context.Context,
		db *sql.DB,
		ak, hk, id string,
	) (aggregatestore.Revision, error)
}

// IncrementAggregateRevision increments the persisted revision of a an
// aggregate instance.
//
// ak is the aggregate handler's identity key, id is the instance ID.
//
// c must be the instance's current revision as persisted, otherwise an
// optimistic concurrency conflict has occurred, the revision is not saved
// and ErrConflict is returned.
func (t *transaction) IncrementAggregateRevision(
	ctx context.Context,
	hk string,
	id string,
	c aggregatestore.Revision,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	return errors.New("not implemented")
}

// aggregateStoreRepository is an implementation of aggregatestore.Repository
// that stores aggregate state in an SQL database.
type aggregateStoreRepository struct {
}

// LoadRevision loads the current revision of an aggregate instance.
//
// ak is the aggregate handler's identity key, id is the instance ID.
func (r *aggregateStoreRepository) LoadRevision(
	ctx context.Context,
	hk, id string,
) (aggregatestore.Revision, error) {
	return 0, errors.New("not implemented")
}
