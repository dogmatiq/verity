package postgres

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
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	res := sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO infix.aggregate_metadata (
			app_key,
			handler_key,
			instance_id,
			instance_exists,
			last_destroyed_by,
			begin_offset,
			end_offset
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7
		) ON CONFLICT (app_key, handler_key, instance_id) DO NOTHING`,
		ak,
		md.HandlerKey,
		md.InstanceID,
		md.InstanceExists,
		md.LastDestroyedBy,
		md.BeginOffset,
		md.EndOffset,
	)

	n, err := res.RowsAffected()
	return n == 1, err
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
		`UPDATE infix.aggregate_metadata SET
			revision = revision + 1,
			instance_exists = $1,
			last_destroyed_by = $2,
			begin_offset = $3,
			end_offset = $4
		WHERE app_key = $5
		AND handler_key = $6
		AND instance_id = $7
		AND revision = $8`,
		md.InstanceExists,
		md.LastDestroyedBy,
		md.BeginOffset,
		md.EndOffset,
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
			instance_exists,
			last_destroyed_by,
			begin_offset,
			end_offset
		FROM infix.aggregate_metadata
		WHERE app_key = $1
		AND handler_key = $2
		AND instance_id = $3`,
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
		&md.InstanceExists,
		&md.LastDestroyedBy,
		&md.BeginOffset,
		&md.EndOffset,
	)
	if err == sql.ErrNoRows {
		err = nil
	}

	return md, err
}
