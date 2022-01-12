package postgres

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
		`INSERT INTO verity.aggregate_metadata (
			app_key,
			handler_key,
			instance_id,
			instance_exists,
			last_event_id,
			barrier_event_id
		) VALUES (
			$1, $2, $3, $4, $5, $6
		) ON CONFLICT (app_key, handler_key, instance_id) DO NOTHING`,
		ak,
		md.HandlerKey,
		md.InstanceID,
		md.InstanceExists,
		md.LastEventID,
		md.BarrierEventID,
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
		`UPDATE verity.aggregate_metadata SET
			revision = revision + 1,
			instance_exists = $1,
			last_event_id = $2,
			barrier_event_id = $3
		WHERE app_key = $4
		AND handler_key = $5
		AND instance_id = $6
		AND revision = $7`,
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
		FROM verity.aggregate_metadata
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

	res := sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO verity.aggregate_snapshot (
			app_key,
			handler_key,
			instance_id,
			last_event_id,
			media_type,
			data
		) VALUES (
			$1, $2, $3, $4, $5, $6
		) ON CONFLICT (app_key, handler_key, instance_id) DO NOTHING`,
		ak,
		inst.HandlerKey,
		inst.InstanceID,
		inst.LastEventID,
		inst.Packet.MediaType,
		inst.Packet.Data,
	)

	n, err := res.RowsAffected()
	return n == 1, err
}

// UpdateAggregateSnapshot updates an aggregate snapshot.
//
// It returns false if the row does not exist or inst.Revision is not current.
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
		`UPDATE verity.aggregate_snapshot SET
			media_type = $1,
			data = $2,
			last_event_id = $3
		WHERE app_key = $4
		AND handler_key = $5
		AND instance_id = $6`,
		inst.Packet.MediaType,
		inst.Packet.Data,
		inst.LastEventID,
		ak,
		inst.HandlerKey,
		inst.InstanceID,
	), nil
}

// DeleteAggregateSnapshot deletes an aggregate snapshot.
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
		`DELETE FROM verity.aggregate_snapshot
		WHERE app_key = $1
		AND handler_key = $2
		AND instance_id = $3`,
		ak,
		inst.HandlerKey,
		inst.InstanceID,
	), nil
}

// SelectAggregateSnapshot selects an aggregate snapshot's data.
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
		FROM verity.aggregate_snapshot
		WHERE app_key = $1
		AND handler_key = $2
		AND instance_id = $3`,
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

	if len(inst.Packet.Data) == 0 {
		inst.Packet.Data = nil
	}

	return inst, true, err
}

// createAggregateSchema creates the schema elements for aggregates.
func createAggregateSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE IF NOT EXISTS verity.aggregate_metadata (
			app_key          TEXT NOT NULL,
			handler_key      TEXT NOT NULL,
			instance_id      TEXT NOT NULL,
			revision         BIGINT NOT NULL DEFAULT 1,
			instance_exists  BOOLEAN NOT NULL,
			last_event_id    TEXT NOT NULL,
			barrier_event_id TEXT NOT NULL,

			PRIMARY KEY (app_key, handler_key, instance_id)
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE IF NOT EXISTS verity.aggregate_snapshot (
			app_key     TEXT NOT NULL,
			handler_key TEXT NOT NULL,
			instance_id TEXT NOT NULL,
			last_event_id     TEXT NOT NULL,
			media_type  TEXT NOT NULL,
			data        BYTEA,

			PRIMARY KEY (app_key, handler_key, instance_id)
		)`,
	)
}
