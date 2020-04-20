package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
)

// InsertAggregateMetaData inserts meta-data for an aggregate instance.
//
// It returns false if the row already exists.
func (driver) InsertAggregateMetaData(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	md *aggregatestore.MetaData,
) (bool, error) {
	return insertIgnore(
		ctx,
		tx,
		`INSERT INTO aggregate_metadata SET
			app_key = ?,
			handler_key = ?,
			instance_id = ?,
			min_offset = ?,
			max_offset = ?`,
		ak,
		md.HandlerKey,
		md.InstanceID,
		md.MinOffset,
		md.MaxOffset,
	)
}

// UpdateAggregateMetaData updates meta-data for an aggregate instance.
//
// It returns false if the row does not exist or md.Revision is not current.
func (driver) UpdateAggregateMetaData(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	md *aggregatestore.MetaData,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`UPDATE aggregate_metadata SET
			revision = revision + 1,
			min_offset = ?,
			max_offset = ?
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?
		AND revision = ?`,
		md.MinOffset,
		md.MaxOffset,
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
) (*aggregatestore.MetaData, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			revision,
			min_offset,
			max_offset
		FROM aggregate_metadata
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?`,
		ak,
		hk,
		id,
	)

	md := &aggregatestore.MetaData{
		HandlerKey: hk,
		InstanceID: id,
	}

	err := row.Scan(
		&md.Revision,
		&md.MinOffset,
		&md.MaxOffset,
	)
	if err == sql.ErrNoRows {
		err = nil
	}

	return md, err
}
