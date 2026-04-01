package sqlite

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
		`INSERT INTO process_instance (
			app_key,
			handler_key,
			instance_id,
			data
		) VALUES (
			$1, $2, $3, $4, $5
		) ON CONFLICT (app_key, handler_key, instance_id) DO NOTHING`,
		ak,
		inst.HandlerKey,
		inst.InstanceID,
		inst.Data,
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
		`UPDATE process_instance SET
			revision = revision + 1,
			data = $1
		WHERE app_key = $2
		AND handler_key = $3
		AND instance_id = $4
		AND revision = $5`,
		inst.Data,
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
		WHERE app_key = $1
		AND handler_key = $2
		AND instance_id = $3
		AND revision = $4`,
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
			data
		FROM process_instance
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
		&inst.Data,
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
			app_key     TEXT NOT NULL,
			handler_key TEXT NOT NULL,
			instance_id TEXT NOT NULL,
			revision    INTEGER NOT NULL DEFAULT 1,
			data        BLOB,

			PRIMARY KEY (app_key, handler_key, instance_id)
		)`,
	)
}

// dropOffsetSchema drops the schema elements for processes.
func dropProcessSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS process_instance`)
}
