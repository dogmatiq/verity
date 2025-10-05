package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/verity/internal/x/sqlx"
	"github.com/dogmatiq/verity/persistence"
)

// InsertAggregateMetaData inserts meta-data for an aggregate instance.
//
// It returns false if the row already exists.
func (driver) InsertAggregateMetaData(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	md persistence.AggregateMetaData,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`INSERT INTO aggregate_metadata SET
			app_key = ?,
			handler_key = ?,
			instance_id = ?,
			last_event_id = ?
		ON DUPLICATE KEY UPDATE
			app_key = app_key`, // do nothing
		ak,
		md.HandlerKey,
		md.InstanceID,
		md.LastEventID,
	), nil
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
			last_event_id = ?
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?
		AND revision = ?`,
		md.LastEventID,
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
			last_event_id
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
		&md.LastEventID,
	)
	if err == sql.ErrNoRows {
		err = nil
	}

	return md, err
}

// createAggregateSchema creates schema elements for aggregates.
func createAggregateSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE IF NOT EXISTS aggregate_metadata (
			app_key       VARBINARY(255) NOT NULL,
			handler_key   VARBINARY(255) NOT NULL,
			instance_id   VARBINARY(255) NOT NULL,
			revision      BIGINT NOT NULL DEFAULT 1,
			last_event_id VARBINARY(255) NOT NULL,

			PRIMARY KEY (app_key, handler_key, instance_id)
		) ENGINE=InnoDB`,
	)
}

// dropAggregateSchema drops  schema elements for aggregates.
func dropAggregateSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS aggregate_metadata`)
}
