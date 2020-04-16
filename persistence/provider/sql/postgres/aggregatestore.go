package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
)

// InsertAggregateRevision inserts an aggregate revision for an aggregate
// instance.
//
// It returns false if the row already exists.
func (driver) InsertAggregateRevision(
	ctx context.Context,
	tx *sql.Tx,
	ak, hk, id string,
) (bool, error) {
	return false, errors.New("not implemented")
}

// UpdateAggregateRevision increments an aggregate isntance's revision by 1.
//
// It returns false if the row does not exist or rev is not current.
func (driver) UpdateAggregateRevision(
	ctx context.Context,
	tx *sql.Tx,
	ak, hk, id string,
	rev aggregatestore.Revision,
) (bool, error) {
	return false, errors.New("not implemented")
}
