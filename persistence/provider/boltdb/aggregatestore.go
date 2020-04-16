package boltdb

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
)

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
) (err error) {
	defer bboltx.Recover(&err)

	if err := t.begin(ctx); err != nil {
		return err
	}

	return errors.New("not implemented")
}

// aggregateStoreRepository is an implementation of aggregatestore.Repository
// that stores aggregate state in a BoltDB database.
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