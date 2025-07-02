package postgres

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

	res := sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO verity.process_instance (
			app_key,
			handler_key,
			instance_id,
			media_type,
			data,
			has_ended
		) VALUES (
			$1, $2, $3, $4, $5, $6
		) ON CONFLICT (app_key, handler_key, instance_id) DO NOTHING`,
		ak,
		inst.HandlerKey,
		inst.InstanceID,
		inst.Packet.MediaType,
		inst.Packet.Data,
		inst.HasEnded,
	)

	n, err := res.RowsAffected()
	return n == 1, err
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
		`UPDATE verity.process_instance SET
			revision = revision + 1,
			media_type = $1,
			data = $2,
			has_ended = $3
		WHERE app_key = $4
		AND handler_key = $5
		AND instance_id = $6
		AND revision = $7`,
		inst.Packet.MediaType,
		inst.Packet.Data,
		inst.HasEnded,
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
			data,
			has_ended
		FROM verity.process_instance
		WHERE app_key = $1
		AND handler_key = $2
		AND instance_id = $3`,
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
		&inst.HasEnded,
	)
	if err == sql.ErrNoRows {
		err = nil
	}

	if len(inst.Packet.Data) == 0 {
		inst.Packet.Data = nil
	}

	return inst, err
}

// createOffsetSchema creates the schema elements for processes.
func createProcessSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE IF NOT EXISTS verity.process_instance (
			app_key     TEXT NOT NULL,
			handler_key TEXT NOT NULL,
			instance_id TEXT NOT NULL,
			revision    BIGINT NOT NULL DEFAULT 1,
			media_type  TEXT NOT NULL,
			data        BYTEA,
			has_ended   BOOLEAN NOT NULL,

			PRIMARY KEY (app_key, handler_key, instance_id)
		)`,
	)
}
