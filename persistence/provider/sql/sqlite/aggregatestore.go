package sqlite

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
	defer sqlx.Recover(&err)

	res := sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO aggregate_revision (
			app_key,
			handler_key,
			instance_id
		) VALUES (
			$1, $2, $3
		) ON CONFLICT (app_key, handler_key, instance_id) DO NOTHING`,
		ak,
		hk,
		id,
	)

	n, err := res.RowsAffected()
	return n == 1, err
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
		WHERE app_key = $1
		AND handler_key = $2
		AND instance_id = $3
		AND revision = $4`,
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
		WHERE app_key = $1
		AND handler_key = $2
		AND instance_id = $3`,
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
