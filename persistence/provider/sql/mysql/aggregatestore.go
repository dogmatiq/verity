package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
)

// InsertAggregateRevision inserts an aggregate revision (with a value of 1) for
// an aggregate instance.
//
// It returns false if the row already exists.
func (driver) InsertAggregateRevision(
	ctx context.Context,
	tx *sql.Tx,
	ak, hk, id string,
) (_ bool, err error) {
	return insertIgnore(
		ctx,
		tx,
		`INSERT INTO aggregate_revision SET
			app_key = ?,
			handler_key = ?,
			instance_id = ?`,
		ak,
		hk,
		id,
	)
}

// UpdateAggregateRevision increments an aggregate isntance's revision by 1.
//
// It returns false if the row does not exist or rev is not current.
func (driver) UpdateAggregateRevision(
	ctx context.Context,
	tx *sql.Tx,
	ak, hk, id string,
	rev aggregatestore.Revision,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`UPDATE aggregate_revision SET
			revision = revision + 1
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?
		AND revision = ?`,
		ak,
		hk,
		id,
		rev,
	), nil
}

// SelectAggregateRevision selects an aggregate instance's revision.
func (driver) SelectAggregateRevision(
	ctx context.Context,
	db *sql.DB,
	ak, hk, id string,
) (aggregatestore.Revision, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			r.revision
		FROM aggregate_revision AS r
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?`,
		ak,
		hk,
		id,
	)

	var rev aggregatestore.Revision
	err := row.Scan(&rev)

	if err == sql.ErrNoRows {
		return 0, nil
	}

	return rev, err
}
