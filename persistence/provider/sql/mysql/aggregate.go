package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
)

// InsertAggregateMetaData inserts meta-data for an aggregate instance.
//
// It returns false if the row already exists.
func (driver) InsertAggregateMetaData(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	md persistence.AggregateMetaData,
) (bool, error) {
	return insertIgnore(
		ctx,
		tx,
		`INSERT INTO aggregate_metadata SET
			app_key = ?,
			handler_key = ?,
			instance_id = ?,
			instance_exists = ?,
			last_destroyed_by = ?`,
		ak,
		md.HandlerKey,
		md.InstanceID,
		md.InstanceExists,
		md.LastDestroyedBy,
	)
}

// UpdateAggregateMetaData updates meta-data for an aggregate instance.
//
// It returns false if the row does not exist or md.Revision is not current.
func (driver) UpdateAggregateMetaData(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	md persistence.AggregateMetaData,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`UPDATE aggregate_metadata SET
			revision = revision + 1,
			instance_exists = ?,
			last_destroyed_by = ?
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?
		AND revision = ?`,
		md.InstanceExists,
		md.LastDestroyedBy,
		ak,
		md.HandlerKey,
		md.InstanceID,
		md.Revision,
	), nil
}

// SelectAggregateMetaData selects an aggregate instance's meta-data.
func (driver) SelectAggregateMetaData(
	ctx context.Context,
	db *sql.DB,
	ak, hk, id string,
) (persistence.AggregateMetaData, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			revision,
			instance_exists,
			last_destroyed_by
		FROM aggregate_metadata
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?`,
		ak,
		hk,
		id,
	)

	md := persistence.AggregateMetaData{
		HandlerKey: hk,
		InstanceID: id,
	}

	err := row.Scan(
		&md.Revision,
		&md.InstanceExists,
		&md.LastDestroyedBy,
	)
	if err == sql.ErrNoRows {
		err = nil
	}

	return md, err
}
