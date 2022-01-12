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
			instance_exists = ?,
			last_event_id = ?,
			barrier_event_id = ?
		ON DUPLICATE KEY UPDATE
			app_key = app_key`, // do nothing
		ak,
		md.HandlerKey,
		md.InstanceID,
		md.InstanceExists,
		md.LastEventID,
		md.BarrierEventID,
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
			instance_exists = ?,
			last_event_id = ?,
			barrier_event_id = ?
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?
		AND revision = ?`,
		md.InstanceExists,
		md.LastEventID,
		md.BarrierEventID,
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
			last_event_id,
			barrier_event_id
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
		&md.LastEventID,
		&md.BarrierEventID,
	)
	if err == sql.ErrNoRows {
		err = nil
	}

	return md, err
}

// InsertAggregateSnapshot inserts an aggregate snapshot.
//
// It returns false if the row already exists.
func (driver) InsertAggregateSnapshot(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	inst persistence.AggregateSnapshot,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`INSERT INTO aggregate_snapshot SET
			app_key = ?,
			handler_key = ?,
			instance_id = ?,
			last_event_id = ?,
			media_type = ?,
			data = ?
		ON DUPLICATE KEY UPDATE
			app_key = app_key`, // do nothing
		ak,
		inst.HandlerKey,
		inst.InstanceID,
		inst.LastEventID,
		inst.Packet.MediaType,
		inst.Packet.Data,
	), nil
}

// UpdateAggregateSnapshot updates an aggregate snapshot.
//
// It returns false if the row does not exist.
func (driver) UpdateAggregateSnapshot(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	inst persistence.AggregateSnapshot,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`UPDATE aggregate_snapshot SET
			media_type = ?,
			data = ?,
			last_event_id = ?
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?`,
		inst.Packet.MediaType,
		inst.Packet.Data,
		inst.LastEventID,
		ak,
		inst.HandlerKey,
		inst.InstanceID,
	), nil
}

// DeleteAggregateSnapshot deletes a process instance.
//
// It returns false if the row does not exist or inst.Revision is not current.
func (driver) DeleteAggregateSnapshot(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	inst persistence.AggregateSnapshot,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`DELETE FROM aggregate_snapshot
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?`,
		ak,
		inst.HandlerKey,
		inst.InstanceID,
	), nil
}

// SelectAggregateSnapshot selects a process instance's data.
func (driver) SelectAggregateSnapshot(
	ctx context.Context,
	db *sql.DB,
	ak, hk, id string,
) (persistence.AggregateSnapshot, bool, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			last_event_id,
			media_type,
			data
		FROM aggregate_snapshot
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?`,
		ak,
		hk,
		id,
	)

	inst := persistence.AggregateSnapshot{
		HandlerKey: hk,
		InstanceID: id,
	}

	err := row.Scan(
		&inst.LastEventID,
		&inst.Packet.MediaType,
		&inst.Packet.Data,
	)
	if err == sql.ErrNoRows {
		return inst, false, nil
	}

	return inst, true, err
}

// createAggregateSchema creates schema elements for aggregates.
func createAggregateSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE IF NOT EXISTS aggregate_metadata (
			app_key          VARBINARY(255) NOT NULL,
			handler_key      VARBINARY(255) NOT NULL,
			instance_id      VARBINARY(255) NOT NULL,
			revision         BIGINT NOT NULL DEFAULT 1,
			instance_exists  TINYINT NOT NULL,
			last_event_id    VARBINARY(255) NOT NULL,
			barrier_event_id VARBINARY(255) NOT NULL,

			PRIMARY KEY (app_key, handler_key, instance_id)
		) ENGINE=InnoDB`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE IF NOT EXISTS aggregate_snapshot (
			app_key     VARBINARY(255) NOT NULL,
			handler_key VARBINARY(255) NOT NULL,
			instance_id VARBINARY(255) NOT NULL,
			last_event_id     VARBINARY(255) NOT NULL,
			media_type  VARBINARY(255) NOT NULL,
			data        LONGBLOB,

			PRIMARY KEY (app_key, handler_key, instance_id)
		) ENGINE=InnoDB`,
	)
}

// dropAggregateSchema drops  schema elements for aggregates.
func dropAggregateSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS aggregate_metadata`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS aggregate_snapshot`)
}
