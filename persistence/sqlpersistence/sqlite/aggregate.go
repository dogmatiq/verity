package sqlite

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

	res := sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO aggregate_metadata (
			app_key,
			handler_key,
			instance_id,
			last_event_id
		) VALUES (
			$1, $2, $3, $4
		) ON CONFLICT (app_key, handler_key, instance_id) DO NOTHING`,
		ak,
		md.HandlerKey,
		md.InstanceID,
		md.LastEventID,
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
	md persistence.AggregateMetaData,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`UPDATE aggregate_metadata SET
			revision = revision + 1,
			last_event_id = $1
		WHERE app_key = $2
		AND handler_key = $3
		AND instance_id = $4
		AND revision = $5`,
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
		WHERE app_key = $1
		AND handler_key = $2
		AND instance_id = $3`,
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

// createAggregateSchema creates the schema elements for aggregates.
func createAggregateSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE IF NOT EXISTS aggregate_metadata (
			app_key       TEXT NOT NULL,
			handler_key   TEXT NOT NULL,
			instance_id   TEXT NOT NULL,
			revision      INTEGER NOT NULL DEFAULT 1,
			last_event_id TEXT NOT NULL,

			PRIMARY KEY (app_key, handler_key, instance_id)
		)`,
	)
}

// dropAggregateSchema drops  schema elements for aggregates.
func dropAggregateSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS aggregate_metadata`)
}
