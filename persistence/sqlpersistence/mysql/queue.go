package mysql

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/verity/internal/x/sqlx"
	"github.com/dogmatiq/verity/persistence"
)

// InsertQueueMessage inserts a message in the queue.
//
// It returns false if the row already exists.
func (driver) InsertQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	m persistence.QueueMessage,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`INSERT INTO queue SET
			app_key = ?,
			failure_count = ?,
			next_attempt_at = ?,
			message_id = ?,
			causation_id = ?,
			correlation_id = ?,
			source_app_name = ?,
			source_app_key = ?,
			source_handler_name = ?,
			source_handler_key = ?,
			source_instance_id = ?,
			created_at = ?,
			scheduled_for = ?,
			description = ?,
			portable_name = ?,
			media_type = ?,
			data = ?
		ON DUPLICATE KEY UPDATE
			app_key = app_key`, // do nothing
		ak,
		m.FailureCount,
		m.NextAttemptAt,
		m.Envelope.GetMessageId(),
		m.Envelope.GetCausationId(),
		m.Envelope.GetCorrelationId(),
		m.Envelope.GetSourceApplication().GetName(),
		m.Envelope.GetSourceApplication().GetKey(),
		m.Envelope.GetSourceHandler().GetName(),
		m.Envelope.GetSourceHandler().GetKey(),
		m.Envelope.GetSourceInstanceId(),
		m.Envelope.GetCreatedAt(),
		m.Envelope.GetScheduledFor(),
		m.Envelope.GetDescription(),
		m.Envelope.GetPortableName(),
		m.Envelope.GetMediaType(),
		m.Envelope.GetData(),
	), nil
}

// UpdateQueueMessage updates meta-data about a message that is already on
// the queue.
//
// It returns false if the row does not exist or m.Revision is not current.
func (driver) UpdateQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	m persistence.QueueMessage,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`UPDATE queue SET
			revision = revision + 1,
			failure_count = ?,
			next_attempt_at = ?
		WHERE app_key = ?
		AND message_id = ?
		AND revision = ?`,
		m.FailureCount,
		m.NextAttemptAt,
		ak,
		m.Envelope.GetMessageId(),
		m.Revision,
	), nil
}

// DeleteQueueMessage deletes a message from the queue.
//
// It returns false if the row does not exist or m.Revision is not current.
func (driver) DeleteQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	m persistence.QueueMessage,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`DELETE FROM queue
		WHERE app_key = ?
		AND message_id = ?
		AND revision = ?`,
		ak,
		m.Envelope.GetMessageId(),
		m.Revision,
	), nil
}

// DeleteQueueTimeoutMessagesByProcessInstance deletes timeout messages that
// were produced by a specific process instance.
func (driver) DeleteQueueTimeoutMessagesByProcessInstance(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	inst persistence.ProcessInstance,
) error {
	_, err := tx.ExecContext(
		ctx,
		`DELETE FROM queue
		WHERE app_key = ?
		AND source_handler_key = ?
		AND source_instance_id = ?
		AND scheduled_for != ""`,
		ak,
		inst.HandlerKey,
		inst.InstanceID,
	)

	return err
}

// SelectQueueMessages selects up to n messages from the queue.
func (driver) SelectQueueMessages(
	ctx context.Context,
	db *sql.DB,
	ak string,
	n int,
) (*sql.Rows, error) {
	return db.QueryContext(
		ctx,
		`SELECT
			q.revision,
			q.failure_count,
			q.next_attempt_at,
			q.message_id,
			q.causation_id,
			q.correlation_id,
			q.source_app_name,
			q.source_app_key,
			q.source_handler_name,
			q.source_handler_key,
			q.source_instance_id,
			q.created_at,
			q.scheduled_for,
			q.description,
			q.portable_name,
			q.media_type,
			q.data
		FROM queue AS q
		WHERE q.app_key = ?
		ORDER BY q.next_attempt_at
		LIMIT ?`,
		ak,
		n,
	)
}

// ScanQueueMessage scans the next message from a row-set returned by
// SelectQueueMessages().
func (driver) ScanQueueMessage(
	rows *sql.Rows,
	m *persistence.QueueMessage,
) error {
	var next string

	err := rows.Scan(
		&m.Revision,
		&m.FailureCount,
		&next,
		&m.Envelope.MessageId,
		&m.Envelope.CausationId,
		&m.Envelope.CorrelationId,
		&m.Envelope.SourceApplication.Name,
		&m.Envelope.SourceApplication.Key,
		&m.Envelope.SourceHandler.Name,
		&m.Envelope.SourceHandler.Key,
		&m.Envelope.SourceInstanceId,
		&m.Envelope.CreatedAt,
		&m.Envelope.ScheduledFor,
		&m.Envelope.Description,
		&m.Envelope.PortableName,
		&m.Envelope.MediaType,
		&m.Envelope.Data,
	)
	if err != nil {
		return err
	}

	m.NextAttemptAt, err = time.Parse(timeLayout, next)

	return err
}

// createQueueSchema creates the schema elements for the message queue.
func createQueueSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE IF NOT EXISTS queue (
			app_key             VARBINARY(255) NOT NULL,
			revision            BIGINT UNSIGNED NOT NULL DEFAULT 1,
			failure_count       BIGINT UNSIGNED NOT NULL DEFAULT 0,
			next_attempt_at     TIMESTAMP(6) NOT NULL,
			message_id          VARBINARY(255) NOT NULL,
			causation_id        VARBINARY(255) NOT NULL,
			correlation_id      VARBINARY(255) NOT NULL,
			source_app_name     VARBINARY(255) NOT NULL,
			source_app_key      VARBINARY(255) NOT NULL,
			source_handler_name VARBINARY(255) NOT NULL,
			source_handler_key  VARBINARY(255) NOT NULL,
			source_instance_id  VARBINARY(255) NOT NULL,
			created_at          VARBINARY(255) NOT NULL,
			scheduled_for       VARBINARY(255) NOT NULL,
			description         VARBINARY(255) NOT NULL,
			portable_name       VARBINARY(255) NOT NULL,
			media_type          VARBINARY(255) NOT NULL,
			data                LONGBLOB NOT NULL,

			PRIMARY KEY (app_key, message_id),
			INDEX by_next_attempt (
				app_key,
				next_attempt_at
			),
			INDEX by_source (
				app_key,
				source_handler_key,
				source_instance_id,
				scheduled_for
			)
		) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4`,
	)
}

// dropQueueSchema drops the schema elements for the message queue.
func dropQueueSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS queue`)
}
