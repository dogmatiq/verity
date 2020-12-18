package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/verity/internal/x/sqlx"
	"github.com/dogmatiq/verity/persistence"
)

// InsertProcessInstance inserts a process instance.
//
// It returns false if the row already exists.
func (driver) InsertProcessInstance(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	inst persistence.ProcessInstance,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`INSERT INTO process_instance SET
			app_key = ?,
			handler_key = ?,
			instance_id = ?,
			media_type = ?,
			data = ?
		ON DUPLICATE KEY UPDATE
			app_key = app_key`, // do nothing
		ak,
		inst.HandlerKey,
		inst.InstanceID,
		inst.Packet.MediaType,
		inst.Packet.Data,
	), nil
}

// UpdateProcessInstance updates a process instance.
//
// It returns false if the row does not exist or inst.Revision is not current.
func (driver) UpdateProcessInstance(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	inst persistence.ProcessInstance,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`UPDATE process_instance SET
			revision = revision + 1,
			media_type = ?,
			data = ?
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?
		AND revision = ?`,
		inst.Packet.MediaType,
		inst.Packet.Data,
		ak,
		inst.HandlerKey,
		inst.InstanceID,
		inst.Revision,
	), nil
}

// DeleteProcessInstance deletes a process instance.
//
// It returns false if the row does not exist or inst.Revision is not current.
func (driver) DeleteProcessInstance(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	inst persistence.ProcessInstance,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`DELETE FROM process_instance
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?
		AND revision = ?`,
		ak,
		inst.HandlerKey,
		inst.InstanceID,
		inst.Revision,
	), nil
}

// SelectProcessInstance selects a process instance's data.
func (driver) SelectProcessInstance(
	ctx context.Context,
	db *sql.DB,
	ak, hk, id string,
) (persistence.ProcessInstance, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			revision,
			media_type,
			data
		FROM process_instance
		WHERE app_key = ?
		AND handler_key = ?
		AND instance_id = ?`,
		ak,
		hk,
		id,
	)

	inst := persistence.ProcessInstance{
		HandlerKey: hk,
		InstanceID: id,
	}

	err := row.Scan(
		&inst.Revision,
		&inst.Packet.MediaType,
		&inst.Packet.Data,
	)
	if err == sql.ErrNoRows {
		err = nil
	}

	return inst, err
}

// createOffsetSchema creates the schema elements for processes.
func createProcessSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE IF NOT EXISTS process_instance (
			app_key     VARBINARY(255) NOT NULL,
			handler_key VARBINARY(255) NOT NULL,
			instance_id VARBINARY(255) NOT NULL,
			revision    BIGINT NOT NULL DEFAULT 1,
			media_type  VARBINARY(255) NOT NULL,
			data        LONGBLOB,

			PRIMARY KEY (app_key, handler_key, instance_id)
		) ENGINE=InnoDB`,
	)
}

// dropOffsetSchema drops the schema elements for processes.
func dropProcessSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS process_instance`)
}
